package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66-oss/trackman/notifiers"
	trackmanType "github.com/cloud66-oss/trackman/utils"
	"github.com/cloud66/cli"
	"github.com/sirupsen/logrus"
	"gopkg.in/go-yaml/yaml.v2"
)

const (
	configstoreDirectoryName = "configstore"
)

var cmdFormations = &Command{
	Name:       "formations",
	Build:      buildFormations,
	Short:      "commands to work with formations",
	NeedsStack: true,
	NeedsOrg:   false,
}

func buildFormations() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		{
			Name:   "list",
			Action: runListFormations,
			Usage:  "lists all the formations of a stack.",
			Description: `List all the formations of a stack.
The information contains the name and UUID

Examples:
$ cx formations list -s mystack
$ cx formations list -s mystack foo bar // only show formations foo and bar
`,
		},
		{
			Name:   "create",
			Action: runCreateFormation,
			Usage:  "Create a new formation",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "name",
					Usage: "Formation name",
				},
				cli.StringFlag{
					Name:  "template-repo",
					Usage: "Base Template repository URL",
				},
				cli.StringFlag{
					Name:  "template-branch",
					Usage: "Base Template repository branch",
				},
				cli.StringFlag{
					Name:  "tags",
					Usage: "Formation tags",
				},
			},
		},
		{
			Name:   "fetch",
			Action: runFetchFormation,
			Usage:  "Fetch all stencils of a Formation",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "formation,f",
					Usage: "the formation name",
				},
				cli.StringFlag{
					Name:  "outdir",
					Usage: "Output director for the Formation. Will be created if missing. If not provided, ~/cloud66/formations will be used, suffixed by the formation name",
				},
				cli.BoolFlag{
					Name:  "y",
					Usage: "Answer yes to all confirmation questions",
				},
				cli.BoolFlag{
					Name:  "overwrite",
					Usage: "Overwrite existing files in outdir if present. Default is false and asks for overwrite permissions per file",
				},
			},
		},
		{
			Name:   "commit",
			Action: runCommitFormation,
			Usage:  "Commit all given stencils for a formation back",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "formation,f",
					Usage: "the formation name",
				},
				cli.StringFlag{
					Name:  "dir",
					Usage: "Directory holding the formation stencils. Cannot be used alongside --stencil",
				},
				cli.BoolFlag{
					Name:  "default-folders",
					Usage: "Use ~/cloud66/formations with a formation name suffix as the dir",
				},
				cli.StringFlag{
					Name:  "stencil",
					Usage: "A single stencil file to commit. Cannot be used alongside --dir",
				},
				cli.StringFlag{
					Name:  "message",
					Usage: "Commit message",
				},
			},
		},
		{
			Name:   "deploy",
			Action: runDeployFormation,
			Usage:  "Deploy an existing formation",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "formation,f",
					Usage: "the formation name",
				},
				cli.StringFlag{
					Name:  "snapshot-uid",
					Usage: "[OPTIONAL, DEFAULT: latest] UID of the snapshot to be used. Use 'latest' to use the most recent snapshot",
				},
				cli.BoolTFlag{
					Name:  "use-latest",
					Usage: "[OPTIONAL, DEFAULT: true] use the snapshot's HEAD gitref (and not the ref stored in the for stencil)",
				},
				cli.StringFlag{
					Name:  "workflow,w",
					Usage: "[OPTIONAL] name of the workflow to use for the formation deployment",
				},
				cli.StringFlag{
					Name:  "log-level",
					Usage: "[OPTIONAL, DEFAULT: info] log level. Use debug to see process output",
				},
			},
		},
		{
			Name:  "bundle",
			Usage: "formation bundle commands",
			Subcommands: []cli.Command{
				{
					Name:   "download",
					Action: runBundleDownload,
					Usage:  "Specify the formation to use",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "formation",
							Usage: "Specify the formation to use",
						},
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
						cli.StringFlag{
							Name:  "file",
							Usage: "filename for the bundle file. formation extension will be added",
						},
						cli.BoolFlag{
							Name:  "overwrite",
							Usage: "overwrite existing bundle file is it exists",
						},
					},
				},
				{
					Name:   "upload",
					Usage:  "Upload a formation bundle to a new formation",
					Action: runBundleUpload,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "formation",
							Usage: "Name for the new formation",
						},
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
						cli.StringFlag{
							Name:  "file",
							Usage: "filename for the bundle file",
						},
						cli.StringFlag{
							Name:  "message",
							Usage: "Commit message",
						},
					},
				},
			},
		},
		{
			Name:        "stencils",
			Usage:       "formation stencil commands",
			Subcommands: stencilSubCommands(),
		},
	}

	return base
}

/* Formations */
func runListFormations(c *cli.Context) {
	stack := mustStack(c)
	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()

	var formations []cloud66.Formation
	var err error
	formations, err = client.Formations(stack.Uid, false)
	must(err)

	formationNames := c.Args()

	for idx, i := range formationNames {
		formationNames[idx] = strings.ToLower(i)
	}
	sort.Strings(formationNames)
	if len(formationNames) == 0 {
		printFormationList(w, formations)
	} else {
		// filter out the unwanted formations
		var filteredFormations []cloud66.Formation
		for _, i := range formations {
			sorted := sort.SearchStrings(formationNames, strings.ToLower(i.Name))
			if sorted < len(formationNames) && strings.ToLower(formationNames[sorted]) == strings.ToLower(i.Name) {
				filteredFormations = append(filteredFormations, i)
			}
		}
		printFormationList(w, filteredFormations)
	}
}

func runCreateFormation(c *cli.Context) {
	stack := mustStack(c)

	tags := []string{}
	name := c.String("name")
	templateRepo := c.String("template-repo")
	templateBranch := c.String("template-branch")
	tagList := c.String("tags")
	if tagList != "" {
		tags = strings.Split(tagList, ",")
	}

	_, err := client.CreateFormation(stack.Uid, name, templateRepo, templateBranch, tags)
	if err != nil {
		printFatal(err.Error())
	}

	fmt.Println("Formation created")
}

func runCommitFormation(c *cli.Context) {
	stack := mustStack(c)

	formationName := getArgument(c, "formation")
	if formationName == "" {
		printFatal("No formation provided. Please use --formation to specify a formation")
	}

	var formation *cloud66.Formation
	formations, err := client.Formations(stack.Uid, true)
	must(err)
	for _, innerFormation := range formations {
		if innerFormation.Name == formationName {
			formation = &innerFormation
			break
		}
	}
	if formation == nil {
		printFatal("Formation with name \"%v\" could not be found", formationName)
	}

	dir := getArgument(c, "dir")
	stencilOption := c.String("stencil")
	defaultFolders := c.Bool("default-folders")
	if dir == "" && stencilOption == "" && !defaultFolders {
		printFatal("Either --dir, --stencil or --default-folders should be provided")
	}

	if dir != "" && stencilOption != "" {
		printFatal("Cannot use both --dir and --stencil at the same time")
	}

	if stencilOption != "" && defaultFolders {
		printFatal("Cannot use both --stencil and --default-folders at the same time")
	}

	if defaultFolders {
		dir, err = defaultOutputFolder(formationName, "stencils")
		if err != nil {
			must(err)
		}
	}

	message := c.String("message")
	if message == "" {
		printFatal("No message provided")
	}

	filesToSave := make([]string, 0)
	if dir != "" {
		fileList, err := ioutil.ReadDir(dir)
		if err != nil {
			printFatal("Cannot fetch file list in %s: %s", dir, err.Error())
		}
		for _, file := range fileList {
			filesToSave = append(filesToSave, filepath.Join(dir, file.Name()))
		}
	} else {
		filesToSave = append(filesToSave, stencilOption)
	}

	for _, file := range filesToSave {
		if does, _ := fileExists(file); !does {
			printFatal("Cannot find %s to save", file)
		}
	}

	for _, stencilFile := range filesToSave {
		stencilName := filepath.Base(stencilFile)
		stencil := formation.FindStencil(stencilName)
		if stencil == nil {
			printFatal("No stencil named %s found on the formation", stencilName)
		}

		body, err := ioutil.ReadFile(stencilFile)
		if err != nil {
			printFatal("Failed to read %s: %s", stencilName, err.Error())
		}
		// check to make it we're not pushing rendered files by mistake
		checksum, _ := readMagicComment(stencilFile, "checksum")
		if checksum != "NO_MATCH" {
			if !ask(fmt.Sprintf("Stencil %s contains a checksum which suggests it might be a rendered stencil. Are you sure you are committing the right file? (y/N)", stencilFile), "y") {
				fmt.Println("Exiting")
				os.Exit(0)
			}
		}

		_, err = client.UpdateStencil(stack.Uid, formation.Uid, stencil.Uid, message, body)
		if err != nil {
			printFatal("Failed to commit %s: %s", stencilFile, err.Error())
		}

		fmt.Printf("Saved %s\n", stencilName)
	}

	fmt.Println("Done")
}

func runFetchFormation(c *cli.Context) {
	stack := mustStack(c)

	formationName := getArgument(c, "formation")
	if formationName == "" {
		printFatal("No formation provided. Please use --formation to specify a formation")
	}

	var formation *cloud66.Formation
	formations, err := client.Formations(stack.Uid, true)
	must(err)
	for _, innerFormation := range formations {
		if innerFormation.Name == formationName {
			formation = &innerFormation
			break
		}
	}
	if formation == nil {
		printFatal("Formation with name \"%v\" could not be found", formationName)
	}

	outDir := getArgument(c, "outdir")
	if outDir == "" {
		outDir, err = defaultOutputFolder(formationName, "stencils")
		if err != nil {
			must(err)
		}
	}

	stencilDir, err := filepath.Abs(outDir)
	if err != nil {
		printFatal(err.Error())
	}

	autoConfirm := c.Bool("y")

	if !autoConfirm && !ask(fmt.Sprintf("Fetching formation to %s. y/N? ", stencilDir), "y") {
		fmt.Println("Exiting")
		os.Exit(0)
	}

	if err := os.MkdirAll(stencilDir, os.ModePerm); err != nil {
		printFatal("Unable to create directory %s: %s", outDir, err.Error())
	}

	overwrite := c.Bool("overwrite")

	for _, stencil := range formation.Stencils {
		body := []byte(stencil.Body)
		write := false
		stencilFile := filepath.Join(stencilDir, stencil.Filename)
		if does, _ := fileExists(stencilFile); does {
			if !overwrite {
				write = ask(fmt.Sprintf("%s already exists. Overwrite N/y?", stencil.Filename), "y")
			} else {
				fmt.Printf("Fetching %s to %s\n", stencil.Filename, stencilDir)
				write = true
			}
		} else {
			write = true
		}

		if write {
			if err := ioutil.WriteFile(stencilFile, body, 0644); err != nil {
				printFatal("Writing %s to %s failed: %s", stencil.Filename, stencilDir, err.Error())
			}
		}
	}

	fmt.Printf("\nFormation is available at %s\n", stencilDir)
}

func runDeployFormation(c *cli.Context) {
	stack := mustStack(c)

	formationName := getArgument(c, "formation")
	if formationName == "" {
		printFatal("No formation provided. Please use --formation to specify a formation")
	}

	var formation *cloud66.Formation
	formations, err := client.Formations(stack.Uid, true)
	must(err)
	for _, innerFormation := range formations {
		if innerFormation.Name == formationName {
			formation = &innerFormation
			break
		}
	}
	if formation == nil {
		printFatal("Formation with name \"%v\" could not be found", formationName)
	}

	snapshotUID := c.String("snapshot-uid")
	if snapshotUID == "" {
		snapshotUID = "latest"
	}

	// use HEAD stencil instead of the version in in the snapshot
	useLatest := c.BoolT("use-latest")

	level := logrus.InfoLevel
	logLevel := c.String("log-level")

	if logLevel == "info" {
		level = logrus.InfoLevel
	} else if logLevel == "debug" {
		level = logrus.DebugLevel
	}

	workflowName := getArgument(c, "workflow")
	workflowWrapper, err := client.GetWorkflow(stack.Uid, formation.Uid, snapshotUID, useLatest, workflowName)
	must(err)

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
		printFatal(runErrors.Error())
	}
	if stepErrors != nil {
		printFatal(stepErrors.Error())
	}
}

func runBundleDownload(c *cli.Context) {
	account := mustOrg(c)
	stack := mustStack(c)
	if account.Id != stack.AccountId {
		printFatal("Stack %s is not in account %s. Please make sure your config points to the correct account and that you have specified the correct stack", stack.Name, account.Name)
	}

	formationName := getArgument(c, "formation")
	if formationName == "" {
		printFatal("No formation provided. Please use --formation to specify a formation")
	}

	bundleFile := c.String("file")
	if bundleFile == "" {
		bundleFile = formationName
	}

	if filepath.Ext(bundleFile) != ".formation" {
		bundleFile = bundleFile + ".formation"
	}

	if _, err := os.Stat(bundleFile); err == nil {
		if !c.Bool("overwrite") {
			printFatal("%s already exists", bundleFile)
		}
	}
	var err error
	var envVars []cloud66.StackEnvVar
	envVars, err = client.StackEnvVars(stack.Uid)
	must(err)

	fmt.Println("Fetching bundle from the server...")
	var formations []cloud66.Formation
	formations, err = client.Formations(stack.Uid, true)
	must(err)

	for _, formation := range formations {
		if formation.Name == formationName {
			err = validateFormationForBundleCreation(&formation)
			must(err)

			fmt.Println("Fetching ConfigStore records from the server...")
			bundledConfigStoreRecords, err := downloadBundledConfigStoreRecords(account, stack, &formation)
			must(err)

			bundleFormation(&formation, bundleFile, envVars, bundledConfigStoreRecords)
			return
		}
	}

	printFatal("No formation named '%s' found", formationName)
}

func runBundleUpload(c *cli.Context) {
	account := mustOrg(c)
	stack := mustStack(c)
	if account.Id != stack.AccountId {
		printFatal("Stack %s is not in account %s. Please make sure your config points to the correct account and that you have specified the correct stack", stack.Name, account.Name)
	}

	formationName := getArgument(c, "formation")
	if formationName == "" {
		printFatal("No formation provided. Please use --formation to specify a formation")
	}

	bundleFile := c.String("file")
	if bundleFile == "" {
		bundleFile = formationName + ".formation"
	}

	// untar the bundle
	bundleTopPath, err := ioutil.TempDir("", fmt.Sprintf("%s-formation-bundle-", formationName))
	if err != nil {
		printFatal(err.Error())
	}

	err = Untar(bundleFile, bundleTopPath)
	if err != nil {
		printFatal(err.Error())
	}
	bundlePath := filepath.Join(bundleTopPath, "bundle")
	manifestFile := filepath.Join(bundlePath, "manifest.json")
	message := c.String("message")
	if message == "" {
		printFatal("No message given. Use --message to provide a message for the commit")
	}

	// load the bundle manifest
	fb := loadFormationBundle(manifestFile)

	// verify the presence of the BTRs
	err = verifyBtrPresence(fb)
	if err != nil {
		printFatal(err.Error())
	}

	// create the formation and populate it with the stencils and policies
	formation, err := createAndUploadFormations(fb, formationName, stack, bundlePath, message)
	if err != nil {
		printFatal(err.Error())
	}

	// add the environment variables
	err = uploadEnvironmentVariables(fb, formation, stack, bundlePath)
	if err != nil {
		printFatal(err.Error())
	}

	fmt.Println("Adding ConfigStore records")
	err = handleBundleUploadConfigStoreRecords(fb, account, stack, formation, bundlePath)
	if err != nil {
		printFatal(err.Error())
	}
	fmt.Println("Added ConfigStore records")
}

func validateFormationForBundleCreation(formation *cloud66.Formation) error {
	for _, stencil := range formation.Stencils {
		index := formation.FindIndexByRepoAndBranch(stencil.BtrRepo, stencil.BtrBranch)
		if index == -1 {
			return fmt.Errorf("The Base Template Repository of stencil %s (URL: %s, branch: %s) no longer exists upstream. Please make sure it exists and try again", stencil.Filename, stencil.BtrRepo, stencil.BtrBranch)
		}
	}

	return nil
}

func bundleFormation(formation *cloud66.Formation, bundleFile string, envVars []cloud66.StackEnvVar, bundledConfigStoreRecords *cloud66.BundledConfigStoreRecords) {
	// build a temp folder structure
	topDir, err := ioutil.TempDir("", fmt.Sprintf("%s-formation-bundle-", formation.Name))
	if err != nil {
		printFatal(err.Error())
	}
	dir := filepath.Join(topDir, "bundle")

	defer os.RemoveAll(dir)
	stencilsDir := filepath.Join(dir, "stencils")
	err = os.MkdirAll(stencilsDir, os.ModePerm)
	if err != nil {
		printFatal(err.Error())
	}
	policiesDir := filepath.Join(dir, "policies")
	err = os.MkdirAll(policiesDir, os.ModePerm)
	if err != nil {
		printFatal(err.Error())
	}
	transformationsDir := filepath.Join(dir, "transformations")
	err = os.MkdirAll(transformationsDir, os.ModePerm)
	if err != nil {
		printFatal(err.Error())
	}
	workflowDir := filepath.Join(dir, "workflows")
	err = os.MkdirAll(workflowDir, os.ModePerm)
	if err != nil {
		printFatal(err.Error())
	}
	configurationsDir := filepath.Join(dir, "configurations")
	err = os.MkdirAll(configurationsDir, os.ModePerm)
	if err != nil {
		printFatal(err.Error())
	}
	configstoreDir := filepath.Join(dir, configstoreDirectoryName)
	err = os.MkdirAll(configstoreDir, os.ModePerm)
	if err != nil {
		printFatal(err.Error())
	}
	releasesDir := filepath.Join(dir, "helm_releases")
	err = os.MkdirAll(releasesDir, os.ModePerm)
	if err != nil {
		printFatal(err.Error())
	}
	manifestFilename := filepath.Join(dir, "manifest.json")

	// save all the files individually
	// stencils
	fmt.Println("Saving stencils...")
	for _, stencil := range formation.Stencils {
		fileName := filepath.Join(stencilsDir, stencil.Filename)
		file, err := os.Create(fileName)
		defer file.Close()
		if err != nil {
			printFatal(err.Error())
		}

		file.WriteString(stencil.Body)
	}

	// policies
	fmt.Println("Saving policies...")
	for _, policy := range formation.Policies {
		fileName := filepath.Join(policiesDir, policy.Uid+".cop")
		file, err := os.Create(fileName)
		defer file.Close()
		if err != nil {
			printFatal(err.Error())
		}

		file.WriteString(policy.Body)
	}

	// transformations
	fmt.Println("Saving transformations...")
	for _, transformation := range formation.Transformations {
		fileName := filepath.Join(transformationsDir, transformation.Uid+".js")
		file, err := os.Create(fileName)
		defer file.Close()
		if err != nil {
			printFatal(err.Error())
		}

		file.WriteString(transformation.Body)
	}

	// workflow
	fmt.Println("Saving workflows...")
	for _, workflow := range formation.Workflows {
		fileName := filepath.Join(workflowDir, workflow.Name)
		file, err := os.Create(fileName)
		defer file.Close()
		if err != nil {
			printFatal(err.Error())
		}

		file.WriteString(workflow.Body)
	}

	// environment variables
	fmt.Println("Saving Environment Variables...")
	var fileOut string
	for _, envas := range envVars {
		if !envas.Readonly {
			fileOut = fileOut + envas.Key + "=" + envas.Value.(string) + "\n"
		}
	}
	filename := "formation-vars"
	varsPath := filepath.Join(configurationsDir, filename)
	err = ioutil.WriteFile(varsPath, []byte(fileOut), 0600)
	if err != nil {
		printFatal(err.Error())
	}
	configurations := []string{filename}

	fmt.Println("Saving ConfigStore records...")
	filename = "configstore-records.yml"
	configstorePath := filepath.Join(configstoreDir, filename)
	err = saveBundledConfigStoreRecords(bundledConfigStoreRecords, configstorePath)
	if err != nil {
		printFatal(err.Error())
	}
	configstore := []string{filename}

	//add helm releases
	fmt.Println("Saving helm releases...")
	for _, release := range formation.HelmReleases {
		fileName := filepath.Join(releasesDir, release.DisplayName+"-values.yml")
		file, err := os.Create(fileName)
		defer file.Close()
		if err != nil {
			printFatal(err.Error())
		}

		file.WriteString(release.Body)
	}

	// create and save the manifest
	fmt.Println("Saving bundle manifest...")
	manifest := cloud66.CreateFormationBundle(*formation, fmt.Sprintf("cx (%s)", VERSION), configurations, configstore)
	buf, err := json.MarshalIndent(manifest, "", "    ")
	if err != nil {
		printFatal(err.Error())
	}
	manifestFile, err := os.Create(manifestFilename)
	if err != nil {
		printFatal(err.Error())
	}
	defer manifestFile.Close()

	_, err = manifestFile.Write(buf)
	if err != nil {
		printFatal(err.Error())
	}

	// tarball
	err = Tar(dir, bundleFile)
	if err != nil {
		printFatal(err.Error())
	}
	fmt.Printf("Bundle is saved to %s\n", bundleFile)
}

func listFormation(w io.Writer, a cloud66.Formation) {
	ta := a.CreatedAt

	listRec(w,
		a.Uid,
		a.Name,
		a.Tags,
		len(a.Stencils),
		len(a.HelmReleases),
		len(a.Transformations),
		len(a.Policies),
		len(a.Workflows),
		len(a.FormationFilters),
		prettyTime{ta},
		prettyTime{a.UpdatedAt},
	)
}

func printFormationList(w io.Writer, formations []cloud66.Formation) {
	sort.Sort(formationByName(formations))

	listRec(w,
		"UID",
		"NAME",
		"TAGS",
		"STENCILS",
		"HELM CHARTS",
		"TRANSFORMATIONS",
		"POLICIES",
		"WORKFLOWS",
		"FILTERS",
		"CREATED AT",
		"LAST UPDATED")

	for _, a := range formations {
		if a.Name != "" {
			listFormation(w, a)
		}
	}
}

type formationByName []cloud66.Formation

func (a formationByName) Len() int           { return len(a) }
func (a formationByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a formationByName) Less(i, j int) bool { return a[i].Name < a[j].Name }

/* End Formations */

func loadFormationBundle(manifestFile string) *cloud66.FormationBundle {
	bundle, err := os.Open(manifestFile)
	if err != nil {
		printFatal(err.Error())
	}
	defer bundle.Close()

	buff, err := ioutil.ReadAll(bundle)
	if err != nil {
		printFatal(err.Error())
	}

	var fb *cloud66.FormationBundle
	err = json.Unmarshal(buff, &fb)
	if err != nil {
		printFatal(err.Error())
	}
	return fb
}

/* Start Bundle Auxiliary Methods */

func verifyBtrPresence(fb *cloud66.FormationBundle) error {
	fmt.Println("Verifying presence of Base Template Repositories")
	baseTemplates, err := client.ListBaseTemplates()
	if err != nil {
		return err
	}

	resyncBTRs := make([]*cloud66.BaseTemplate, 0)
	for _, btr := range fb.BaseTemplates {
		var remoteBTR *cloud66.BaseTemplate
		for _, rb := range baseTemplates {
			if strings.TrimSpace(rb.GitRepo) == strings.TrimSpace(btr.Repo) && strings.TrimSpace(rb.GitBranch) == strings.TrimSpace(btr.Branch) {
				remoteBTR = &rb
				break
			}
		}

		if remoteBTR == nil {
			return fmt.Errorf("Base Template Repository with URL %s and branch %s does not exist upstream. Please make sure it is created, and try again.\n", btr.Repo, btr.Branch)
		}

		if remoteBTR.StatusCode != 6 {
			resyncBTRs = append(resyncBTRs, remoteBTR)
		}
	}

	if len(resyncBTRs) > 0 {
		fmt.Println("Waiting for the new Base Template Repositories to be verified")
		ready := false
		for ready == false {
			time.Sleep(100 * time.Millisecond)
			ready = true
			baseTemplates, err = client.ListBaseTemplates()
			for _, b := range resyncBTRs {
				for _, rb := range baseTemplates {
					if b.Uid == rb.Uid && rb.StatusCode != 5 && rb.StatusCode != 6 && rb.StatusCode != 7 {
						ready = false
						break
					}
				}
			}
		}
	}

	return nil
}

func createAndUploadFormations(fb *cloud66.FormationBundle, formationName string, stack *cloud66.Stack, bundlePath string, message string) (*cloud66.Formation, error) {
	fmt.Printf("Creating %s formation...\n", formationName)

	baseTemplates := getTemplateList(fb)
	formation, err := client.CreateFormationMultiBtr(stack.Uid, formationName, baseTemplates, fb.Tags)
	if err != nil {
		return nil, err
	}
	fmt.Println("Formation created")

	for _, baseTemplate := range fb.BaseTemplates {
		// add stencils
		err = uploadStencils(baseTemplate, formation, stack, bundlePath, message)
		if err != nil {
			return nil, err
		}

	}

	// add the policies
	err = uploadPolicies(fb, formation, stack, bundlePath, message)
	if err != nil {
		printFatal(err.Error())
	}

	// add the transformations
	err = uploadTransformations(fb, formation, stack, bundlePath, message)
	if err != nil {
		printFatal(err.Error())
	}

	// add helm releases
	err = uploadHelmReleases(fb, formation, stack, bundlePath, message)
	if err != nil {
		printFatal(err.Error())
	}

	// add workflow
	err = uploadWorkflows(fb, formation, stack, bundlePath, message)
	if err != nil {
		printFatal(err.Error())
	}

	return formation, nil
}

func uploadStencils(baseTemplate *cloud66.BundleBaseTemplates, formation *cloud66.Formation, stack *cloud66.Stack, bundlePath string, message string) error {
	// add stencils
	fmt.Println("Adding stencils...")
	var err error
	stencils := make([]*cloud66.Stencil, len(baseTemplate.Stencils))
	for idx, stencil := range baseTemplate.Stencils {
		stencils[idx], err = stencil.AsStencil(bundlePath)
		if err != nil {
			return err
		}
	}

	btrIndex := formation.FindIndexByRepoAndBranch(baseTemplate.Repo, baseTemplate.Branch)
	if btrIndex == -1 {
		return errors.New("base template repository not found")

	}

	_, err = client.AddStencils(stack.Uid, formation.Uid, formation.BaseTemplates[btrIndex].Uid, stencils, message)
	if err != nil {
		return err
	}

	fmt.Println("Stencils are queued for addition")

	return nil
}

func uploadPolicies(bundleFormation *cloud66.FormationBundle, formation *cloud66.Formation, stack *cloud66.Stack, bundlePath string, message string) error {
	// add policies
	fmt.Println("Adding policies...")
	policies := make([]*cloud66.Policy, 0)
	for _, policy := range bundleFormation.Policies {
		polItem, err := policy.AsPolicy(bundlePath)
		if err != nil {
			return err
		}
		policies = append(policies, polItem)
		if err != nil {
			return err
		}
	}
	_, err := client.AddPolicies(stack.Uid, formation.Uid, policies, message)
	if err != nil {
		return err
	}
	fmt.Println("Policies added")
	return nil
}

func uploadTransformations(bundleFormation *cloud66.FormationBundle, formation *cloud66.Formation, stack *cloud66.Stack, bundlePath string, message string) error {
	// add transformations
	fmt.Println("Adding transformations...")
	transformations := make([]*cloud66.Transformation, 0)
	for _, transformation := range bundleFormation.Transformations {
		trItem, err := transformation.AsTransformation(bundlePath)
		if err != nil {
			return err
		}
		transformations = append(transformations, trItem)
		if err != nil {
			return err
		}
	}
	_, err := client.AddTransformations(stack.Uid, formation.Uid, transformations, message)
	if err != nil {
		return err
	}
	fmt.Println("Transformations added")
	return nil
}

func uploadHelmReleases(fb *cloud66.FormationBundle, formation *cloud66.Formation, stack *cloud66.Stack, bundlePath string, message string) error {
	var err error
	fmt.Println("Adding helm releases...")
	helmReleases := make([]*cloud66.HelmRelease, len(fb.HelmReleases))
	for idx, release := range fb.HelmReleases {
		helmReleases[idx], err = release.AsRelease(bundlePath)
		if err != nil {
			return err
		}
	}
	_, err = client.AddHelmReleases(stack.Uid, formation.Uid, helmReleases, message)
	if err != nil {
		return err
	}
	fmt.Println("Helm Releases added")
	return nil
}

func uploadEnvironmentVariables(fb *cloud66.FormationBundle, formation *cloud66.Formation, stack *cloud66.Stack, bundlePath string) error {
	envVars := make(map[string]string, 0)
	for _, envFileName := range fb.Configurations {
		file, err := os.Open(filepath.Join(bundlePath, "configurations", envFileName))
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			env := strings.Split(scanner.Text(), "=")
			if len(env) < 2 {
				fmt.Print("Wrong environment variable value\n")
				continue
			}
			envVars[env[0]] = strings.Join(env[1:], "=")
		}

		if err := scanner.Err(); err != nil {
			return err
		}
	}
	for key, value := range envVars {
		asyncResult, err := client.StackEnvVarNew(stack.Uid, key, value)
		if err != nil {
			if err.Error() == "Another environment variable with the same key exists. Use PUT to change it." {
				fmt.Printf("Failed to add the %s environment variable because it already exists\n", key)
			} else {
				return err
			}
		}
		if asyncResult != nil {
			_, err = endEnvVarSet(asyncResult.Id, stack.Uid)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func handleBundleUploadConfigStoreRecords(fb *cloud66.FormationBundle, account *cloud66.Account, stack *cloud66.Stack, formation *cloud66.Formation, bundlePath string) error {
	configStoreRecords, err := parseConfigStoreEntriesFromFormationBundle(fb, bundlePath)
	if err != nil {
		return err
	}

	err = uploadConfigStoreRecords(configStoreRecords, account, stack, formation)
	if err != nil {
		return err
	}

	return nil
}

func parseConfigStoreEntriesFromFormationBundle(fb *cloud66.FormationBundle, bundlePath string) (*cloud66.BundledConfigStoreRecords, error) {
	configStoreRecordArray := make([]cloud66.BundledConfigStoreRecord, 0)
	for _, fileName := range fb.ConfigStore {
		configStoreRecords, err := parseConfigStoreEntriesFromFile(filepath.Join(bundlePath, configstoreDirectoryName, fileName))
		if err != nil {
			return nil, err
		}
		// NOTE: this may give you records with duplicate keys
		configStoreRecordArray = append(configStoreRecordArray, configStoreRecords.Records...)
	}

	result := cloud66.BundledConfigStoreRecords{Records: configStoreRecordArray}
	return &result, nil
}

func parseConfigStoreEntriesFromFile(filePath string) (*cloud66.BundledConfigStoreRecords, error) {
	marshalledResult, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var unmarshalledResult cloud66.BundledConfigStoreRecords
	err = yaml.Unmarshal(marshalledResult, &unmarshalledResult)
	if err != nil {
		return nil, err
	}

	return &unmarshalledResult, nil
}

func uploadConfigStoreRecords(configStoreRecords *cloud66.BundledConfigStoreRecords, account *cloud66.Account, stack *cloud66.Stack, formation *cloud66.Formation) error {
	for _, record := range configStoreRecords.Records {
		var namespace string
		switch record.Scope {
		case cloud66.BundledConfigStoreAccountScope:
			namespace = account.ConfigStoreNamespace
		case cloud66.BundledConfigStoreStackScope:
			namespace = stack.ConfigStoreNamespace
		default:
			return fmt.Errorf("ConfigStore record scope %s is not supported. Supported values are: %s, %s.", record.Scope, cloud66.BundledConfigStoreAccountScope, cloud66.BundledConfigStoreStackScope)
		}

		_, err := client.CreateConfigStoreRecord(namespace, &record.ConfigStoreRecord)
		if err != nil {
			if strings.Contains(err.Error(), "Duplicate entry") {
				fmt.Printf("Failed to add the %s ConfigStore record because it already exists\n", record.Key)
			} else {
				return err
			}
		}
	}

	return nil
}

func downloadBundledConfigStoreRecords(account *cloud66.Account, stack *cloud66.Stack, formation *cloud66.Formation) (*cloud66.BundledConfigStoreRecords, error) {
	allRecords := make([]cloud66.BundledConfigStoreRecord, 0)

	accountRecords, err := client.GetConfigStoreRecords(account.ConfigStoreNamespace)
	if err != nil {
		return nil, err
	}
	for _, record := range accountRecords {
		allRecords = append(allRecords, cloud66.BundledConfigStoreRecord{ConfigStoreRecord: record, Scope: cloud66.BundledConfigStoreAccountScope})
	}

	stackRecords, err := client.GetConfigStoreRecords(stack.ConfigStoreNamespace)
	if err != nil {
		return nil, err
	}
	for _, record := range stackRecords {
		allRecords = append(allRecords, cloud66.BundledConfigStoreRecord{ConfigStoreRecord: record, Scope: cloud66.BundledConfigStoreStackScope})
	}

	result := cloud66.BundledConfigStoreRecords{Records: allRecords}
	return &result, nil
}

func saveBundledConfigStoreRecords(bundledConfigStoreRecords *cloud66.BundledConfigStoreRecords, filepath string) error {
	marshalledOutput, err := yaml.Marshal(&bundledConfigStoreRecords)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(filepath, marshalledOutput, 0600)
	if err != nil {
		return err
	}

	return nil
}

func uploadWorkflows(fb *cloud66.FormationBundle, formation *cloud66.Formation, stack *cloud66.Stack, bundlePath string, message string) error {
	fmt.Println("Adding workflow...")
	for _, workflow := range fb.Workflows {

		workflowItem, err := workflow.AsWorkflow(bundlePath)

		if err != nil {
			return err
		}

		_, err = client.AddWorkflow(stack.Uid, formation.Uid, workflowItem, message)
		if err != nil {
			return err
		}
	}
	fmt.Println("Workflows added")
	return nil
}

func getTemplateList(fb *cloud66.FormationBundle) []*cloud66.BaseTemplate {
	btrs := make([]*cloud66.BaseTemplate, 0)
	for _, value := range fb.BaseTemplates {
		btrs = append(btrs, &cloud66.BaseTemplate{
			Name:      value.Name,
			GitRepo:   value.Repo,
			GitBranch: value.Branch,
		})

	}
	return btrs
}

/* End Bundle Auxiliary Methods */
