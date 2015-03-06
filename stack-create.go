package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/cloud66/cloud66"
	"github.com/cloud66/cx/term"

	"github.com/cloud66/cli"
)

func runCreateStack(c *cli.Context) {
	name := c.String("name")
	environment := c.String("environment")
	serviceYamlFile := c.String("service_yaml")
	manifestYamlFile := c.String("manifest_yaml")
	manifestYaml := ""

	if len(name) < 5 {
		printFatal("name is required and must be at least 5 characters long")
	}
	if environment == "" {
		printFatal("environment is required")
	}

	// handle service yaml file
	if serviceYamlFile == "" {
		printFatal("service_yaml file path is required")
	} else {
		serviceYamlFile = expandPath(serviceYamlFile)
	}
	serviceYamlBytes, err := ioutil.ReadFile(serviceYamlFile)
	must(err)
	serviceYaml := string(serviceYamlBytes)

	accountInfo, err := currentAccountInfo()
	must(err)

	fmt.Printf("Using account: %s\n", accountInfo.Owner)

	targetOptions := make(map[string]string)
	if manifestYamlFile != "" {

		fmt.Println("Using supplied manifest file")
		manifestYamlFile = expandPath(manifestYamlFile)
		manifestYamlBytes, err := ioutil.ReadFile(manifestYamlFile)
		must(err)
		manifestYaml = string(manifestYamlBytes)
	} else {

		fmt.Println("Note: No manifest provided; for additional options you can provide your own manifest with this command")
		targetCloud, err := askForCloud(*accountInfo)
		must(err)
		targetOptions["cloud"] = targetCloud

		targetRegion, targetSize, err := askForSizeAndRegion(targetCloud)
		must(err)
		targetOptions["region"] = targetRegion
		targetOptions["size"] = targetSize

		targetBuildType, err := askForBuildType()
		must(err)
		targetOptions["build_type"] = targetBuildType
	}

	asyncId, err := startCreateStack(name, environment, serviceYaml, manifestYaml, targetOptions)
	must(err)

	// now we fetch the corresponding stack
	stack, err := client.StackInfoWithEnvironment(name, environment)
	must(err)

	// wait for the stack analysis to complete
	_, err = endCreateStack(*asyncId, stack.Uid)
	must(err)
	fmt.Printf("\nStack created; Build starting...\n\n")

	err = initiateBuildStack(stack.Uid)
	must(err)

	stack, err = waitForBuild(stack)
	must(err)
	fmt.Println("Stack build completed successfully!")
}

func startCreateStack(name, environment, serviceYaml, manifestYaml string, targetOptions map[string]string) (*int, error) {
	asyncRes, err := client.CreateStack(name, environment, serviceYaml, manifestYaml, targetOptions)
	if err != nil {
		return nil, err
	}
	return &asyncRes.Id, err
}

func endCreateStack(asyncId int, stackUid string) (*cloud66.GenericResponse, error) {
	return client.WaitStackAsyncAction(asyncId, stackUid, 5*time.Second, 20*time.Minute, false)
}

func initiateBuildStack(stackUid string) error {
	_, err := client.RedeployStack(stackUid, "", "")
	return err
}

func waitForBuild(stack *cloud66.Stack) (*cloud66.Stack, error) {

	// log output
	StartListen(stack)
	return nil, nil
	// return client.WaitStackBuild(stackUid)
}

func askForCloud(accountInfo cloud66.Account) (string, error) {
	if len(accountInfo.UsedClouds) == 0 {
		return "", errors.New("No available cloud providers in current account, please add via the Cloud 66 UI")
	}

	fmt.Println("\nPlease select your target cloud:")
	cloudMap := make(map[string]string)
	for index, usedCloud := range accountInfo.UsedClouds {
		stringIndex := strconv.Itoa(index + 1)
		cloudMap[stringIndex] = usedCloud
		fmt.Printf("%s. %s\n", stringIndex, usedCloud)
	}
	if term.IsTerminal(os.Stdin) {
		fmt.Printf("> ")
	}
	var selection string
	if _, err := fmt.Scanln(&selection); err != nil {
		printFatal(err.Error())
	}
	if cloudMap[selection] == "" {
		return "", errors.New("Invalid selection!")
	}
	return cloudMap[selection], nil
}

func askForSizeAndRegion(cloudName string) (string, string, error) {
	cloudInfo, err := client.GetCloudInfo(cloudName)
	if err != nil {
		return "", "", err
	}

	fmt.Println("\nPlease select your cloud region:")
	regionMap := make(map[string]string)
	for index, region := range cloudInfo.Regions {
		stringIndex := strconv.Itoa(index + 1)
		regionMap[stringIndex] = region.Id
		fmt.Printf("%s. %s\n", stringIndex, region.Name)
	}
	if term.IsTerminal(os.Stdin) {
		fmt.Printf("> ")
	}
	var selection string
	if _, err := fmt.Scanln(&selection); err != nil {
		printFatal(err.Error())
	}
	if regionMap[selection] == "" {
		return "", "", errors.New("Invalid selection!")
	}
	// store for return
	region := regionMap[selection]

	fmt.Println("\nPlease select your server size:")
	sizeMap := make(map[string]string)
	for index, serverSize := range cloudInfo.ServerSizes {
		stringIndex := strconv.Itoa(index + 1)
		sizeMap[stringIndex] = serverSize.Id
		fmt.Printf("%s. %s\n", stringIndex, serverSize.Name)
	}
	if term.IsTerminal(os.Stdin) {
		fmt.Printf("> ")
	}
	if _, err := fmt.Scanln(&selection); err != nil {
		printFatal(err.Error())
	}
	if sizeMap[selection] == "" {
		return "", "", errors.New("Invalid selection!")
	}
	// store for return
	size := sizeMap[selection]

	return region, size, nil
}

// values are standalone or dedicated
func askForBuildType() (string, error) {
	fmt.Println("\nPlease select your server build type: ")

	serverMap := make(map[string]string)
	serverMap["1"] = "multi"
	fmt.Printf("1. %s\n", "Each database type on its own server")
	serverMap["2"] = "single"
	fmt.Printf("2. %s\n", "Everything on a single server (not recommended for production)")
	if term.IsTerminal(os.Stdin) {
		fmt.Printf("> ")
	}
	var selection string
	if _, err := fmt.Scanln(&selection); err != nil {
		printFatal(err.Error())
	}

	if serverMap[selection] == "" {
		return "", errors.New("Invalid selection!")
	}
	return serverMap[selection], nil
}

func currentAccountInfo() (*cloud66.Account, error) {
	accountInfos, err := client.AccountInfos()
	if err != nil {
		return nil, err
	}

	if len(accountInfos) < 1 {
		printFatal("User associated with this request has no accounts")
	}
	for _, accountInfo := range accountInfos {
		if accountInfo.CurrentAccount {
			return &accountInfo, nil
		}
	}
	return nil, errors.New("No account found for current user")
}
