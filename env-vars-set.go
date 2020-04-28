package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cloud66-oss/cloud66"

	"github.com/cloud66/cli"
)

func runEnvVarsSet(c *cli.Context) {
	if len(c.Args()) != 1 {
		cli.ShowSubcommandHelp(c)
		os.Exit(2)
	}

	// when to apply the env var changes
	flagApplyStrategy := c.String("apply-strategy")
	if flagApplyStrategy == "" {
		// default the apply-strategy
		flagApplyStrategy = "immediately"
	} else if flagApplyStrategy != "immediately" && flagApplyStrategy != "deployment" {
		printFatal("The selected apply-strategy is not valid. Please choose from \"immediately\" or \"deployment\"")
	}

	kv := c.Args()[0]
	kvs := strings.Split(kv, "=")
	if len(kvs) < 2 {
		cli.ShowSubcommandHelp(c)
		os.Exit(2)
	}

	key := kvs[0]
	value := strings.Join(kvs[1:], "=")

	stack := mustStack(c)

	envVars, err := client.StackEnvVars(stack.Uid)
	must(err)

	existing := false
	for _, i := range envVars {
		if i.Key == key {
			if i.Readonly == true {
				printFatal("The selected environment variable is readonly")
			} else {
				existing = true
			}
		}
	}

	if flagApplyStrategy == "immediately" {
		fmt.Println("Please wait while your changes are applied immediately...")
	} else {
		fmt.Println("Your changes will be applied during your next deployment!")
	}

	asyncId, err := startEnvVarSet(stack.Uid, key, value, existing, flagApplyStrategy)
	if err != nil {
		printFatal(err.Error())
	}
	genericRes, err := endEnvVarSet(*asyncId, stack.Uid)
	if err != nil {
		printFatal(err.Error())
	}
	printGenericResponse(*genericRes)

	return
}

func startEnvVarSet(stackUid string, key string, value string, existing bool, applyStrategy string) (*int, error) {
	var (
		asyncRes *cloud66.AsyncResult
		err      error
	)
	if existing {
		asyncRes, err = client.StackEnvVarSet(stackUid, key, value, applyStrategy)
	} else {
		asyncRes, err = client.StackEnvVarNew(stackUid, key, value, applyStrategy)
	}
	if err != nil {
		return nil, err
	}
	return &asyncRes.Id, err
}

func endEnvVarSet(asyncId int, stackUid string) (*cloud66.GenericResponse, error) {
	return client.WaitStackAsyncAction(asyncId, stackUid, 3*time.Second, 20*time.Minute, true)
}
