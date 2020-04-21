package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66-oss/trackman/notifiers"
	trackmanType "github.com/cloud66-oss/trackman/utils"
	"github.com/cloud66/cli"
	"github.com/sirupsen/logrus"
)

const (
	MAX_BACKOFF  = 600
	QUEUE_NAME   = "skycap_render_queue"
	TASK_SUCCESS = "success"
	TASK_FAIL    = "fail"
	TASK_ACK     = "ack"
)

var cmdSkycap = &Command{
	Name:       "skycap",
	Build:      buildSkycap,
	NeedsStack: false,
	NeedsOrg:   false,
	Short:      "all skycap specific commands",
}

type skycapRenderQueuePayload struct {
	TaskUUID  string             `json:"task_uuid"`
	Formation *cloud66.Formation `json:"formation"`
	Snapshot  *cloud66.Snapshot  `json:"snapshot"`
	Stack     *cloud66.Stack     `json:"stack"`
	Workflow  *cloud66.Workflow  `json:"workflow"`
}

var skycapListenDeployRunning bool

func buildSkycap() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		cli.Command{
			Name:  "listen",
			Usage: "listen to all skycap events",
			Subcommands: []cli.Command{
				cli.Command{
					Name:   "deploy",
					Usage:  "deploy automatically after each snapshot",
					Action: runSkycapListenDeploy,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "log-level",
							Value: "info",
							Usage: "[OPTIONAL] log level. Use debug to see process output",
						},
						cli.DurationFlag{
							Name:  "interval",
							Value: 10 * time.Second,
							Usage: "[OPTIONAL] Queue check interval. Must be bigger than 5 seconds",
						},
					},
				},
			},
		},
	}

	return base
}

func runSkycapListenDeploy(c *cli.Context) {
	skycapListenDeployRunning = false
	level := logrus.InfoLevel
	logLevel := c.String("log-level")

	if logLevel == "info" {
		level = logrus.InfoLevel
	} else if logLevel == "debug" {
		level = logrus.DebugLevel
	}

	interval := c.Duration("interval")
	if interval == 0 {
		interval = 5 * time.Second
	}
	if interval < 5*time.Second {
		printFatal("Interval must be 5 seconds or longer")
	}

	printInfo("Listening for Skycap snapshot events...")
	close := make(chan os.Signal, 1)
	signal.Notify(close, os.Interrupt, syscall.SIGTERM)

	operation := func() error {
		msg, err := client.PopQueue(QUEUE_NAME)
		if err != nil {
			return err
		}
		if msg != nil {
			doRender(msg, level)
		}

		return nil
	}

	exp := backoff.NewExponentialBackOff()
	exp.InitialInterval = interval
	exp.MaxElapsedTime = MAX_BACKOFF * time.Second

	ticker := backoff.NewTicker(exp)

	for {
		select {
		case <-ticker.C:
			if skycapListenDeployRunning {
				continue
			}
			if err := operation(); err != nil {
				printError(err.Error())
			} else {
				exp.Reset()
			}
		case <-close:
			printInfo("Exiting...")
			os.Exit(0)
		}
	}
}

func doRender(msg json.RawMessage, level logrus.Level) {
	skycapListenDeployRunning = true
	defer func() {
		skycapListenDeployRunning = false
	}()

	var payload skycapRenderQueuePayload
	err := json.Unmarshal(msg, &payload)
	if err != nil {
		printError("Error in fetching items from the queue %v\n", err)
		return
	}

	if payload.Formation == nil || payload.Snapshot == nil || payload.Stack == nil {
		return
	}

	var workflowName string
	if payload.Workflow == nil {
		workflowName = ""
	} else {
		workflowName = payload.Workflow.Name
	}

	var taskMsg string
	if payload.TaskUUID == "" {
		taskMsg = "No Task"
	} else {
		taskMsg = payload.TaskUUID
		// ack the message
		updateTask(payload.TaskUUID, TASK_ACK, "")
	}

	if payload.Workflow == nil {
		printInfo(fmt.Sprintf("Running task %s formation %s, using snapshot %s (taken on %s) for stack %s\n", taskMsg, payload.Formation.Name, payload.Snapshot.Uid, payload.Snapshot.UpdatedAt, payload.Stack.Name))
	} else {
		printInfo(fmt.Sprintf("Running task %s formation %s, workflow %s using snapshot %s (taken on %s) for stack %s\n", taskMsg, payload.Formation.Name, payload.Workflow.Name, payload.Snapshot.Uid, payload.Snapshot.UpdatedAt, payload.Stack.Name))
	}

	workflowWrapper, err := client.GetWorkflow(payload.Stack.Uid, payload.Formation.Uid, payload.Snapshot.Uid, true, workflowName)
	if err != nil {
		printError("Error in fetching default workflow %s\n", err)
		updateTask(payload.TaskUUID, TASK_FAIL, err.Error())
		return
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, trackmanType.CtxLogLevel, level)

	reader := bytes.NewReader(workflowWrapper.Workflow)
	options := &trackmanType.WorkflowOptions{
		Notifier:    notifiers.ConsoleNotify,
		Concurrency: runtime.NumCPU() - 1,
		Timeout:     10 * time.Minute,
	}

	var runErr string
	workflow, err := trackmanType.LoadWorkflowFromReader(ctx, options, reader)
	if err != nil {
		runErr = err.Error()
		fmt.Println(runErr)
		updateTask(payload.TaskUUID, TASK_FAIL, runErr)
		return
	}
	runErrors, stepErrors := workflow.Run(ctx)
	var stepErr string
	if runErrors != nil {
		runErr = runErrors.Error()
		fmt.Println(runErr)
	}
	if stepErrors != nil {
		stepErr = stepErrors.Error()
		fmt.Println(stepErr)
	}

	if runErrors != nil || stepErrors != nil {
		updateTask(payload.TaskUUID, TASK_FAIL, fmt.Sprintf("Run Errors %s\nStep Errors: %s\n", runErr, stepErr))
	}

	if stepErrors != nil {

	}

	if stepErrors != nil || runErrors != nil {
		printError("Deployment failed or has errors")
	} else {
		printInfo("Finished deployment")
		updateTask(payload.TaskUUID, TASK_SUCCESS, "")
	}
}

func updateTask(taskUUID string, state string, runResult string) {
	if taskUUID == "" {
		printInfo("No task to update")
		return
	}
	_, updateErr := client.UpdateQueue(QUEUE_NAME, taskUUID, state, runResult)
	if updateErr != nil {
		printError("Failed to update the task with results %s\n", updateErr.Error())
	}
}
