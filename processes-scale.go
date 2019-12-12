package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66/cli"
)

func runProcessScale(c *cli.Context) {
	flagServer := c.String("server")
	flagName := c.String("name")
	if flagName == "" || len(c.Args()) != 1 {
		cli.ShowSubcommandHelp(c)
		os.Exit(2)
	}
	count := c.Args()[0]
	count = strings.Replace(count, "[", "", -1)
	count = strings.Replace(count, "]", "", -1)
	count = strings.Replace(count, " ", "", -1)

	stack := mustStack(c)
	if stack.Framework == "docker" {
		printFatal("Stack '" + stack.Name + "' is a docker stack; 'cx processes' is not supported, use 'cx services' instead")
	}

	// fetch servers info
	servers, err := client.Servers(stack.Uid)
	must(err)

	var serverUid *string
	if flagServer == "" {
		serverUid = nil
	} else {
		server, err := findServer(servers, flagServer)
		must(err)
		if server == nil {
			printFatal("Server '" + flagServer + "' not found")
		}
		fmt.Printf("Server: %s\n", server.Name)
		// filter servers collection down
		servers = make([]cloud66.Server, 1)
		servers[0] = *server
		serverUid = &server.Uid
	}

	// param for api call
	serverCountDesired := make(map[string]int)

	var absoluteCount int
	if strings.ContainsAny(count, "+ & -") {

		// fetch process information for existing counts
		process, err := client.GetProcess(stack.Uid, flagName, serverUid)
		must(err)

		serverCountCurrent := process.ServerProcessCount
		relativeCount, _ := strconv.Atoi(count)

		for _, server := range servers {
			if _, present := serverCountCurrent[server.Name]; present {
				serverCountDesired[server.Uid] = relativeCount + serverCountCurrent[server.Name]
			} else {
				serverCountDesired[server.Uid] = relativeCount
			}
		}
	} else {
		absoluteCount, _ = strconv.Atoi(count)
		for _, server := range servers {
			serverCountDesired[server.Uid] = absoluteCount
		}
	}

	// validate non < 0
	for serverUid, count := range serverCountDesired {
		if count < 0 {
			serverCountDesired[serverUid] = 0
		}
	}

	fmt.Println("Scaling your '" + flagName + "' process")

	var asyncId *int
	asyncId, err = startProcessScale(stack.Uid, flagName, serverCountDesired)

	must(err)
	genericRes, err := endProcessScale(*asyncId, stack.Uid)
	must(err)
	printGenericResponse(*genericRes)
	return
}

func startProcessScale(stackUid string, processName string, serverCount map[string]int) (*int, error) {
	asyncRes, err := client.ScaleProcess(stackUid, processName, serverCount)
	must(err)
	return &asyncRes.Id, err
}

func endProcessScale(asyncId int, stackUid string) (*cloud66.GenericResponse, error) {
	return client.WaitStackAsyncAction(asyncId, stackUid, 5*time.Second, 20*time.Minute, true)
}
