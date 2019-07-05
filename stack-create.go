package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66-oss/cx/term"

	"github.com/cloud66/cli"
)

func runCreateStack(c *cli.Context) {
	name := strings.Trim(c.String("name"), " ")
	environment := c.String("environment")
	serviceYamlFile := c.String("service_yaml")
	manifestYamlFile := c.String("manifest_yaml")
	manifestYaml := ""

	if len(name) < 5 {
		printFatal("name is required and must be at least 5 characters long")
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

	accountInfo := mustOrg(c)

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
		targetOptions["cloud"] = targetCloud.Id
		targetOptions["key_name"] = targetCloud.KeyName

		targetRegion, targetSize, err := askForSizeAndRegion(*targetCloud)
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

	err = initiateStackBuild(stack.Uid)
	must(err)

	// logging output
	go StartListen(stack)

	stack, err = WaitStackBuild(stack.Uid, false)
	must(err)
	fmt.Println("Done.")
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

func initiateStackBuild(stackUid string) error {
	_, err := client.RedeployStack(stackUid, "", nil)
	return err
}

func askForCloud(accountInfo cloud66.Account) (*cloud66.Cloud, error) {
	cloudsInfo, err := client.GetCloudsInfo()
	if err != nil {
		return nil, errors.New("No available cloud providers in current account, please add via the Cloud 66 UI")
	}

	fmt.Println("\nPlease select your target cloud:")
	cloudMap := make(map[string]cloud66.Cloud)
	for index, usedCloud := range cloudsInfo {
		stringIndex := strconv.Itoa(index + 1)
		cloudMap[stringIndex] = usedCloud
		fmt.Printf("%s. %s (%s)\n", stringIndex, usedCloud.Id, usedCloud.KeyName)
	}
	if term.IsTerminal(os.Stdin) {
		fmt.Printf("> ")
	}
	var selection string
	if _, err := fmt.Scanln(&selection); err != nil {
		printFatal(err.Error())
	}
	cloudInfo := cloudMap[selection]
	if cloudInfo.Id == "" {
		return nil, errors.New("Invalid selection!")
	}
	return &cloudInfo, nil
}

func askForSizeAndRegion(cloudInfo cloud66.Cloud) (string, string, error) {
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

func WaitStackBuild(stackUid string, visualFeedback bool) (*cloud66.Stack, error) {

	// timout timer
	timeout := 3 * time.Hour
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()

	// handle interrupts
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	// perform checks
	updateTicker := time.NewTicker(1 * time.Minute)
	defer updateTicker.Stop()

	// perform visual feedbacks
	visualTicker := time.NewTicker(20 * time.Second)
	defer visualTicker.Stop()

	// capture results
	completeChan := make(chan *cloud66.Stack)
	errorChan := make(chan error)

	go func() {
		for {
			select {
			case <-visualTicker.C:
				if visualFeedback {
					fmt.Printf(".")
				}

			case <-updateTicker.C:
				// fetch the current status of the async action
				stack, err := client.FindStackByUid(stackUid)
				if err != nil {
					errorChan <- err
					return
				}
				// check for a result!
				if (stack.StatusCode == 1 || stack.StatusCode == 2 || stack.StatusCode == 7) &&
					(stack.HealthCode == 2 || stack.HealthCode == 3 || stack.HealthCode == 4) {
					completeChan <- stack
					return
				}

			case <-timeoutTimer.C:
				// too late! abort!
				errorChan <- errors.New("timed-out after " + strconv.FormatInt(int64(timeout)/int64(time.Second), 10) + " second(s)")
				return

			case <-termChan:
				// too late! abort!
				errorChan <- errors.New("Aborted!")
				return
			}
		}
	}()

	for {
		select {
		case stack := <-completeChan:
			return stack, nil

		case err := <-errorChan:
			return nil, err
		}
	}
}
