package main

import (
	"fmt"
	"time"

	"github.com/cloud66/cli"
)

func runRedeploy(c *cli.Context) {
	stack := mustStack(c)

	// confirmation is needed if the stack is production
	if stack.Environment == "production" && !c.Bool("y") {
		mustConfirm("This is a production stack. Proceed with deployment? [yes/N]", "yes")
	}

	if len(c.StringSlice("service")) > 0 {
		fmt.Printf("Deploying service(s): ")
		for i, service := range c.StringSlice("service") {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf(service)
		}
		fmt.Printf("\n")
	}

	gitRef := c.String("git-ref")
	services := c.StringSlice("service")
	deployStrategy := c.String("deploy-strategy")

	if deployStrategy != "" {
		if deployStrategy != "serial" && deployStrategy != "parallel" &&
			deployStrategy != "rolling" && deployStrategy != "fast" {
			printFatal("The \"deploy strategy\" argument can only be \"serial\", \"parallel\", \"rolling\" or \"fast\"")
		}
		if deployStrategy == "fast" && stack.Backend != "kubernetes" {
			printFatal("The \"fast\" deploy strategy only applies to Maestro stacks")
		}
		if deployStrategy == "rolling" && stack.Framework != "rails" && stack.Framework != "rack" {
			printFatal("The \"rolling\" deploy strategy only applies to Rails/Rack stacks")
		}
	}

	if len(services) > 0 && stack.Framework != "docker" {
		printFatal("The \"service\" argument only applies to Maestro stacks")
	}

	result, err := client.RedeployStack(stack.Uid, gitRef, deployStrategy, services)
	must(err)

	if !c.Bool("listen") || result.Queued {
		// its queued - just message and exit
		fmt.Println(result.Message)
	} else {
		if result.AsyncActionId != nil {
			// wait for the async action to complete
			genericRes, err := client.WaitStackAsyncAction(*(result.AsyncActionId), stack.Uid, 15*time.Second, 120*time.Minute, true)
			if err != nil {
				printFatal(err.Error())
			}
			printGenericResponse(*genericRes)
		} else {
			// tail the logs
			go StartListen(stack)

			stack, err = WaitStackBuild(stack.Uid, false)
			must(err)

			if stack.HealthCode == 2 || stack.HealthCode == 4 || stack.StatusCode == 2 || stack.StatusCode == 7 {
				printFatal("Completed with some errors!")
			} else {
				fmt.Println("Completed successfully!")
			}
		}
	}
}
