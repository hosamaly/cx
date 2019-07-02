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
	"github.com/fsnotify/fsnotify"
	"github.com/mgutz/ansi"
	"github.com/sirupsen/logrus"
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
					Usage: "Output director for the Formation. Will be created if missing",
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
			Name:  "stencils",
			Usage: "formation stencil commands",
			Subcommands: []cli.Command{
				cli.Command{
					Name:   "list",
					Usage:  "List all formation stencils",
					Action: runListStencils,
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
							Name:  "output,o",
							Usage: "tailor output view (standard|wide)",
						},
					},
					Description: `Fetch all formation stencils and their templates
Examples:
$ cx formations stencils list --formation foo
$ cx formations stencils list --formation bar
`,
				},
				{
					Name:   "show",
					Usage:  "Shows the content of a single stencil",
					Action: runShowStencil,
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
							Name:  "stencil",
							Usage: "Stencil filename",
						},
					},
				},
				{
					Name:   "render",
					Usage:  "Renders a stencil based on the given content without committing it into the Formation git repository",
					Action: runRenderStencil,
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
							Name:  "stencil-file",
							Usage: "Stencil filename. This can be a full file path but the file name should be identical to the one available as part of the Formation",
						},
						cli.StringFlag{
							Name:  "stencil-folder",
							Usage: "Render all files within the folder. Cannot be used with stencil-file at the same time",
						},
						cli.StringFlag{
							Name:  "snapshot",
							Usage: "Snapshot ID. Default uses the latest snapshot",
						},
						cli.StringFlag{
							Name:  "output",
							Usage: "Full file name and path to save the rendered stencil. If missing it will output to stdout",
						},
						cli.BoolFlag{
							Name:  "watch",
							Usage: "Watches the file or the folder for changes and renders every time there is a new change",
						},
					},
				},
				{
					Name:   "add",
					Usage:  "Add a stencil to the formation",
					Action: runAddStencil,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "formation",
							Usage: "Specify the formation to use",
						},
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "Full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
						cli.StringFlag{
							Name:  "stencil",
							Usage: "Stencil file",
						},
						cli.StringFlag{
							Name:  "service",
							Usage: "Service context of the stencil, if applicable",
						},
						cli.StringFlag{
							Name:  "template",
							Usage: "Template filename",
						},
						cli.IntFlag{
							Name:  "sequence",
							Usage: "Stencil sequence",
						},
						cli.StringFlag{
							Name:  "message",
							Usage: "Commit message",
						},
						cli.StringFlag{
							Name:  "tags",
							Usage: "Comma separated tags",
						},
					},
				},
			},
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

	formationName := c.String("formation")
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

	dir := c.String("dir")
	stencilOption := c.String("stencil")
	if dir == "" && stencilOption == "" {
		printFatal("Either --dir or --stencil should be provided")
	}

	if dir != "" && stencilOption != "" {
		printFatal("Cannot use both --dir and --stencil at the same time")
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

	formationName := c.String("formation")
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

	outDir := c.String("outdir")
	if outDir == "" {
		printFatal("No output directory provided. Use outdir option")
		cli.ShowSubcommandHelp(c)
	}

	stencilDir := filepath.Join(outDir, "stencils")
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
				write = true
			}
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

	formationName := c.String("formation")
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

	workflowWrapper, err := client.GetWorkflow(stack.Uid, formation.Uid, snapshotUID, useLatest)
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
	stack := mustStack(c)

	formationName := c.String("formation")
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
			bundleFormation(formation, bundleFile, envVars)
			return
		}
	}

	printFatal("No formation named '%s' found", formationName)
}

func runBundleUpload(c *cli.Context) {
	stack := mustStack(c)

	formationName := c.String("formation")
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
}

func bundleFormation(formation cloud66.Formation, bundleFile string, envVars []cloud66.StackEnvVar) {
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
	stencilGroupsDir := filepath.Join(dir, "stencil_groups")
	err = os.MkdirAll(stencilGroupsDir, os.ModePerm)
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
	configurationsDir := filepath.Join(dir, "configurations")
	err = os.MkdirAll(configurationsDir, os.ModePerm)
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

	// stencilgroups
	fmt.Println("Saving stencil groups...")
	for _, stencilGroup := range formation.StencilGroups {
		fileName := filepath.Join(stencilGroupsDir, stencilGroup.Uid+".json")
		file, err := os.Create(fileName)
		defer file.Close()
		if err != nil {
			printFatal(err.Error())
		}

		file.WriteString(stencilGroup.Rules)
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

	//add helm releases
	fmt.Println("Saving helm releases...")
	for _, release := range formation.HelmReleases {
		fileName := filepath.Join(releasesDir, release.ChartName+"-values.yml")
		file, err := os.Create(fileName)
		defer file.Close()
		if err != nil {
			printFatal(err.Error())
		}

		file.WriteString(release.Body)
	}

	// create and save the manifest
	fmt.Println("Saving bundle manifest...")
	manifest := cloud66.CreateFormationBundle(formation, fmt.Sprintf("cx (%s)", VERSION), configurations)
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
		len(a.StencilGroups),
		len(a.Policies),
		a.BaseTemplates,
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
		"STENCIL GROUPS",
		"POLICIES",
		"BASE TEMPLATE",
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

/* Stencils */
func runListStencils(c *cli.Context) {
	stack := mustStack(c)
	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()

	formationName := c.String("formation")
	if formationName == "" {
		printFatal("No formation provided. Please use --formation to specify a formation")
	}

	var formations []cloud66.Formation
	var err error
	formations, err = client.Formations(stack.Uid, true)
	must(err)

	output := c.String("output")
	if output == "" {
		output = "standard"
	}

	for _, formation := range formations {
		if formation.Name == formationName {
			printStencils(w, formation, output)
			return
		}
	}

	printFatal("No formation named '%s' found", formationName)
}

func runRenderStencil(c *cli.Context) {
	stack := mustStack(c)

	formationName := c.String("formation")
	if formationName == "" {
		printFatal("No formation provided. Please use --formation to specify a formation")
	}

	stencilFolder := c.String("stencil-folder")
	stencilFilename := c.String("stencil-file")
	if stencilFilename == "" && stencilFolder == "" {
		printFatal("No stencil file or folder provided. Please use --stencil-file or --stencil-folder to specify a stencil file or folder")
	}
	if stencilFolder != "" && stencilFilename != "" {
		printFatal("Both --stencil-file and --stencil-folder provided. Please use only one")
	}

	output := c.String("output")
	snapshotID := c.String("snapshot")
	stdout := (output == "")
	watch := c.Bool("watch")

	if watch && stdout {
		printFatal("Cannot use --watch without --output")
	}

	filesToRender := make([]string, 0)
	if stencilFolder != "" {
		fileList, err := ioutil.ReadDir(stencilFolder)
		if err != nil {
			printFatal("Failed to fetch all files from folder %s: %s", stencilFolder, err.Error())
		}
		for _, file := range fileList {
			filesToRender = append(filesToRender, filepath.Join(stencilFolder, file.Name()))
		}
	} else {
		filesToRender = append(filesToRender, stencilFilename)
	}

	var outdir string
	// if output is defined, then make sure we have a folder for it
	if !stdout {
		if stencilFolder != "" {
			outdir = output
		} else {
			outdir = filepath.Dir(output)
		}

		os.MkdirAll(outdir, os.ModePerm)
	}

	for _, stencil := range filesToRender {
		file := filepath.Base(stencil)
		if stencilFolder != "" {
			output = filepath.Join(outdir, file)
		}

		if !stdout {
			fmt.Printf("Rendering %s to %s\n", file, output)
		}
		// output filename is sequenced if provided. otherwise, it's concatenated
		renderStencil(stencil, formationName, stack, output, snapshotID)
	}

	if watch {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			printFatal("Failed to setup a file watcher: %s", err.Error())
		}
		defer watcher.Close()

		done := make(chan bool)

		fmt.Println("Watching for changes...")

		go func() {
			for {
				select {
				case event, ok := <-watcher.Events:
					if !ok {
						return
					}
					if event.Op&fsnotify.Write == fsnotify.Write {
						changedFile := filepath.Base(event.Name)
						output := filepath.Join(outdir, changedFile)
						fmt.Printf("Rendering %s to %s\n", changedFile, output)
						renderStencil(event.Name, formationName, stack, output, snapshotID)
					}
				case err, ok := <-watcher.Errors:
					if !ok {
						return
					}
					printFatal("Error during file change event: %s", err.Error())
				}
			}
		}()

		for _, file := range filesToRender {
			err = watcher.Add(file)
			if err != nil {
				printFatal("Failed to add a watch for %s: %s", file, err.Error())
			}
		}
		<-done
	}
}

func renderStencil(stencilFilename string, formationName string, stack *cloud66.Stack, output string, snapshotID string) {
	if does, _ := fileExists(stencilFilename); !does {
		printFatal("Cannot find %s", stencilFilename)
	}
	// find the file. it should exist
	stencilName := filepath.Base(stencilFilename)

	// find the snapshot
	var snapshotUID string
	if snapshotID == "" || snapshotID == "latest" {
		snapshots, err := client.Snapshots(stack.Uid)
		must(err)
		sort.Sort(snapshotsByDate(snapshots))
		if len(snapshots) == 0 {
			printFatal("No snapshots found")
		}

		snapshotUID = snapshots[0].Uid
	} else {
		snapshotUID = snapshotID
	}

	var formations []cloud66.Formation
	var err error
	formations, err = client.Formations(stack.Uid, false)
	must(err)

	stencilUID := ""
	formationUID := ""

	for _, formation := range formations {
		if formation.Name == formationName {
			formationUID = formation.Uid
			for _, stencil := range formation.Stencils {
				if stencil.Filename == stencilName {
					// we have the stencil get the ID
					stencilUID = stencil.Uid
				}
			}

			if stencilUID == "" {
				printFatal("No stencil named '%s' found", stencilName)
			}
		}
	}

	if formationUID == "" {
		printFatal("No formation named '%s' found", formationName)
	}

	// Read file to byte slice
	body, err := ioutil.ReadFile(stencilFilename)
	if err != nil {
		printFatal("Failed to read %s: %s", stencilFilename, err.Error())
	}

	var renders *cloud66.Renders
	renders, err = client.RenderStencil(stack.Uid, snapshotUID, formationUID, stencilUID, body)
	must(err)

	foundErrors := renders.Errors()
	if len(foundErrors) != 0 {
		fmt.Fprintln(os.Stderr, ansi.Color("Error during rendering of stencils:", "red+h"))
		for _, renderError := range foundErrors {
			fmt.Fprintf(os.Stderr, ansi.Color(fmt.Sprintf("\t%s in %s\n", renderError.Text, renderError.Stencil), "red+h"))
		}

		return
	}

	foundWarnings := renders.Warnings()
	if len(foundWarnings) != 0 {
		fmt.Fprintln(os.Stderr, ansi.Color("Warning during rendering of stencils:", "yellow"))
		for _, renderError := range foundWarnings {
			fmt.Fprintf(os.Stderr, ansi.Color(fmt.Sprintf("\t%s in %s\n", renderError.Text, renderError.Stencil), "yellow"))
		}

		return
	}

	// content
	for _, v := range renders.Stencils {
		// to a file
		if output != "" {
			err = ioutil.WriteFile(output, []byte(v.Content), 0644)
			if err != nil {
				printFatal(err.Error())
			}
		} else {
			// concatenate
			fmt.Printf("%s---\n", v.Content)
		}
	}
}

func runShowStencil(c *cli.Context) {
	stack := mustStack(c)

	formationName := c.String("formation")
	if formationName == "" {
		printFatal("No formation provided. Please use --formation to specify a formation")
	}

	stencilName := c.String("stencil")
	if stencilName == "" {
		printFatal("No stencil name provided. Please use --stencil to specify a stencil")
	}

	var formations []cloud66.Formation
	var err error
	formations, err = client.Formations(stack.Uid, true)
	must(err)

	foundStencil := false

	for _, formation := range formations {
		if formation.Name == formationName {
			for _, stencil := range formation.Stencils {
				if stencil.Filename == stencilName {
					printStencil(stencil)
					foundStencil = true
				}
			}

			if !foundStencil {
				printFatal("No stencil named '%s' found", stencilName)
			}
			return
		}
	}

	printFatal("No formation named '%s' found", formationName)
}

func runAddStencil(c *cli.Context) {
	stack := mustStack(c)

	formationName := c.String("formation")
	if formationName == "" {
		printFatal("No formation provided. Please use --formation to specify a formation")
	}

	stencilFile := c.String("stencil")
	if stencilFile == "" {
		printFatal("No stencil filename provided. Please use --stencil to specify a stencil file")
	}

	btrUuid := c.String("base-template")
	if btrUuid == "" {
		printFatal("No base template uuid provided. Please use --base-template to specify a stencil file")
	}

	tags := []string{}
	contextID := c.String("service")
	template := c.String("template")
	sequence := c.Int("sequence")
	message := c.String("message")
	tagList := c.String("tags")
	if tagList != "" {
		tags = strings.Split(tagList, ",")
	}

	var formations []cloud66.Formation
	var err error
	formations, err = client.Formations(stack.Uid, true)
	must(err)
	var foundFormation cloud66.Formation

	for _, formation := range formations {
		if formation.Name == formationName {
			for _, stencil := range formation.Stencils {
				if stencil.Filename == stencilFile {
					// there is a stencil with the same name. abort
					printFatal("Another stencil with the same name is found. You can use the update command to update it")
					return
				}
			}
			foundFormation = formation
		}
	}

	if err := addStencil(stack, &foundFormation, btrUuid, stencilFile, contextID, template, sequence, message, tags); err != nil {
		printFatal(err.Error())
	}

	fmt.Println("Stencil was added to formation")
}

func printStencils(w io.Writer, formation cloud66.Formation, output string) {
	stencils := formation.Stencils
	sort.Sort(stencilBySequence(stencils))

	if output == "standard" {
		listRec(w,
			"UID",
			"FILENAME",
			"TAGS",
			"CREATED AT",
			"LAST UPDATED")
	} else {
		listRec(w,
			"UID",
			"FILENAME",
			"SERVICE",
			"TAGS",
			"TEMPLATE",
			"GITFILE",
			"INLINE",
			"CREATED AT",
			"LAST UPDATED")
	}

	for _, a := range stencils {
		listStencil(w, a, output)
	}
}

func printStencil(stencil cloud66.Stencil) {
	var buffer bytes.Buffer

	buffer.WriteString(stencil.Body)
	fmt.Print(buffer.String())
}

func addStencil(stack *cloud66.Stack, formation *cloud66.Formation, btrUuid string, stencilFile string, contextID string, templateFilename string, sequence int, message string, tags []string) error {
	body, err := ioutil.ReadFile(stencilFile)
	if err != nil {
		return err
	}

	remoteFilename := filepath.Base(stencilFile)
	stencil := &cloud66.Stencil{
		Filename:         remoteFilename,
		TemplateFilename: templateFilename,
		ContextID:        contextID,
		Tags:             tags,
		Body:             string(body),
		Sequence:         sequence,
	}

	_, err = client.AddStencils(stack.Uid, formation.Uid, btrUuid, []*cloud66.Stencil{stencil}, message)
	if err != nil {
		return err
	}

	return nil
}

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

func listStencil(w io.Writer, a cloud66.Stencil, output string) {
	ta := a.CreatedAt

	if output == "standard" {
		listRec(w,
			a.Uid,
			a.Filename,
			a.Tags,
			prettyTime{ta},
			prettyTime{a.UpdatedAt},
		)
	} else {
		listRec(w,
			a.Uid,
			a.Filename,
			a.ContextID,
			a.Tags,
			a.TemplateFilename,
			a.GitfilePath,
			a.Inline,
			prettyTime{ta},
			prettyTime{a.UpdatedAt})
	}
}

type stencilBySequence []cloud66.Stencil

func (a stencilBySequence) Len() int           { return len(a) }
func (a stencilBySequence) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a stencilBySequence) Less(i, j int) bool { return a[i].Sequence < a[j].Sequence }

/* End Stencils */

/* Start Bundle Auxiliary Methods */

func verifyBtrPresence(fb *cloud66.FormationBundle) error {
	fmt.Print("Verifying the presence of the Base Template Repository\n")
	baseTemplates, err := client.ListBaseTemplates()
	if err != nil {
		return err
	}
	addedBTRs := make([]*cloud66.BaseTemplate, 0)
	for _, btr := range fb.BaseTemplates {
		var btrPresent bool = false
		for _, remoteBTR := range baseTemplates {
			if strings.TrimSpace(remoteBTR.GitRepo) == strings.TrimSpace(btr.Repo) && strings.TrimSpace(remoteBTR.GitBranch) == strings.TrimSpace(btr.Branch) && remoteBTR.StatusCode == 6 {
				btrPresent = true
				break
			}
		}
		if !btrPresent {
			baseTemplate := &cloud66.BaseTemplate{
				Name:      btr.Name,
				GitRepo:   btr.Repo,
				GitBranch: btr.Branch,
			}
			baseTemplate, err := client.CreateBaseTemplate(baseTemplate)
			if err != nil {
				return err
			}
			addedBTRs = append(addedBTRs, baseTemplate)
		}
	}
	if len(addedBTRs) > 0 {
		//Waiting for the new BTRs to be verified
		fmt.Print("Waiting for the new Base Template Repositories to be verified\n")
		ready := false
		for ready == false {
			time.Sleep(100 * time.Millisecond)
			ready = true
			baseTemplates, err = client.ListBaseTemplates()
			for _, btr := range addedBTRs {
				for _, remoteBTR := range baseTemplates {
					if btr.Uid == remoteBTR.Uid && remoteBTR.StatusCode != 5 && remoteBTR.StatusCode != 6 && remoteBTR.StatusCode != 7 {
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

	// add stencil groups
	err = uploadStencilGroups(fb, formation, stack, bundlePath, message)
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
	fmt.Println("Stencils added")

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
	fmt.Println("Adding environment variables")
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
				fmt.Print("Failed to add the ", key, " environment variable because already present\n")
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

func uploadStencilGroups(fb *cloud66.FormationBundle, formation *cloud66.Formation, stack *cloud66.Stack, bundlePath string, message string) error {
	var err error
	fmt.Println("Adding stencil groups...")
	stencilGroups := make([]*cloud66.StencilGroup, len(fb.StencilGroups))
	for idx, group := range fb.StencilGroups {
		stencilGroups[idx], err = group.AsStencilGroup(bundlePath)
		if err != nil {
			return err
		}
	}
	_, err = client.AddStencilGroups(stack.Uid, formation.Uid, stencilGroups, message)
	if err != nil {
		return err
	}
	fmt.Println("Stencil Groups added")
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
