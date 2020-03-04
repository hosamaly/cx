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

const MAX_BACKOFF = 600

var cmdSkycap = &Command{
	Name:       "skycap",
	Build:      buildSkycap,
	NeedsStack: false,
	NeedsOrg:   false,
	Short:      "all skycap specific commands",
}

type skycapRenderQueuePayload struct {
	Formation *cloud66.Formation
	Snapshot  *cloud66.Snapshot
	Stack     *cloud66.Stack
	Workflow  *cloud66.Workflow
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
		msg, err := client.PopQueue("skycap_render_queue", false)
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

	if payload.Workflow == nil {
		printInfo(fmt.Sprintf("Running formation %s, using snapshot %s (taken on %s) for stack %s\n", payload.Formation.Name, payload.Snapshot.Uid, payload.Snapshot.UpdatedAt, payload.Stack.Name))
	} else {
		printInfo(fmt.Sprintf("Running formation %s, workflow %s using snapshot %s (taken on %s) for stack %s\n", payload.Formation.Name, payload.Workflow.Name, payload.Snapshot.Uid, payload.Snapshot.UpdatedAt, payload.Stack.Name))
	}

	workflowWrapper, err := client.GetWorkflow(payload.Stack.Uid, payload.Formation.Uid, payload.Snapshot.Uid, true, workflowName)
	if err != nil {
		printError("Error in fetching default workflow %s\n", err)
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

	workflow, err := trackmanType.LoadWorkflowFromReader(ctx, options, reader)
	runErrors, stepErrors := workflow.Run(ctx)
	if runErrors != nil {
		fmt.Println(runErrors.Error())
	}
	if stepErrors != nil {
		fmt.Println(stepErrors.Error())
	}

	if stepErrors != nil || runErrors != nil {
		printError("Deployment failed or has errors")
	} else {
		printInfo("Finished deployment")
	}

}
