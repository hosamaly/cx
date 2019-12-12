package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"text/tabwriter"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66/cli"
)

var cmdSnapshots = &Command{
	Name:       "snapshots",
	Build:      buildSnapshots,
	Short:      "commands to work with snapshots",
	NeedsStack: true,
	NeedsOrg:   false,
}

func buildSnapshots() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		cli.Command{
			Name:   "list",
			Action: runSnapshots,
			Usage:  "lists all the snapshots of a stack.",
			Description: `List all the snapshots of a stack.
The information contains the triggers, snapshot UUID and date/time

Examples:
$ cx snapshots list -s mystack
`,
		},
		cli.Command{
			Name:   "render",
			Action: runRenders,
			Usage:  "renders the given formation based on the requested snapshot",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "snapshot",
					Usage: "UID of the snapshot to be used. Use 'latest' to use the most recent snapshnot",
				},
				cli.StringFlag{
					Name:  "formation",
					Usage: "UID of the formation to be used",
				},
				cli.StringSliceFlag{
					Name:  "files",
					Value: &cli.StringSlice{},
					Usage: "files to pull. If not provided all files will be pulled",
				},
				cli.BoolTFlag{
					Name:  "latest",
					Usage: "use the HEAD for stencils. True by default. If false, it would use the snapshot's gitref",
				},
				cli.StringFlag{
					Name:  "outdir",
					Usage: "if provided, it will save the rendered files in this directory",
				},
				cli.BoolFlag{
					Name:  "ignore-errors",
					Usage: "if set, it will return anything that can be rendered and ignores the errors",
				},
				cli.BoolFlag{
					Name:  "ignore-warnings",
					Usage: "if set, it will return anything that can be rendered and ignores the warnings",
				},
				cli.StringFlag{
					Name:  "filter",
					Usage: "name of the formation filter to be used in the render",
				},
			},
			Description: `Render the requested files for the given formation and snapshot

			Examples:
			$ cx snapshots render -s mystack --formation fm-xxxx --snapshot sn-yyyy --latest --files foo.yaml --files bar.yml
			`,
		},
	}

	return base
}

func runSnapshots(c *cli.Context) {
	stack := mustStack(c)
	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()

	var snapshots []cloud66.Snapshot
	var err error
	snapshots, err = client.Snapshots(stack.Uid)
	must(err)

	snapshotNames := c.Args()

	for idx, i := range snapshotNames {
		snapshotNames[idx] = strings.ToLower(i)
	}
	sort.Strings(snapshotNames)
	if len(snapshotNames) == 0 {
		printSnapshotList(w, snapshots)
	} else {
		// filter out the unwanted snapshots
		var filteredSnapshots []cloud66.Snapshot
		for _, i := range snapshots {
			sorted := sort.SearchStrings(snapshotNames, strings.ToLower(i.Uid))
			if sorted < len(snapshotNames) && strings.ToLower(snapshotNames[sorted]) == strings.ToLower(i.Uid) {
				filteredSnapshots = append(filteredSnapshots, i)
			}
		}
		printSnapshotList(w, filteredSnapshots)
	}
}

func runRenders(c *cli.Context) {
	stack := mustStack(c)
	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()

	snapshotUID := c.String("snapshot")
	formationUID := c.String("formation")
	requestFiles := c.StringSlice("files")
	useLatest := c.BoolT("latest")
	outdir := c.String("outdir")
	ignoreErrors := c.Bool("ignore-errors")
	ignoreWarnings := c.Bool("ignore-warnings")
	filter := c.String("filter")

	if snapshotUID == "latest" {
		snapshots, err := client.Snapshots(stack.Uid)
		must(err)
		sort.Sort(snapshotsByDate(snapshots))
		if len(snapshots) == 0 {
			printFatal("No snapshots found")
		}

		snapshotUID = snapshots[0].Uid
	}

	var renders *cloud66.Renders
	var err error
	renders, err = client.RenderSnapshot(stack.Uid, snapshotUID, formationUID, requestFiles, useLatest, filter)
	must(err)

	if outdir != "" {
		os.MkdirAll(outdir, os.ModePerm)
	}

	foundErrors := renders.Errors()
	if !ignoreErrors && len(foundErrors) != 0 {
		fmt.Fprintln(os.Stderr, "Error during rendering of stencils:")
		for _, renderError := range foundErrors {
			fmt.Fprintf(os.Stderr, "%s in %s\n", renderError.Text, renderError.Stencil)
		}

		return
	}

	foundWarnings := renders.Warnings()
	if !ignoreWarnings && len(foundWarnings) != 0 {
		fmt.Fprintln(os.Stderr, "Warning during rendering of stencils:")
		for _, renderError := range foundWarnings {
			fmt.Fprintf(os.Stderr, "%s in %s\n", renderError.Text, renderError.Stencil)
		}

		return
	}

	// content
	var buffer bytes.Buffer
	for idx, v := range renders.Stencils {
		filename := filepath.Join(outdir, fmt.Sprintf("%03d_%s", idx+1, v.Filename))
		if outdir != "" {
			content := generateYamlComment(v.Filename, snapshotUID, formationUID, v.Sequence) + v.Content
			err = ioutil.WriteFile(filename, []byte(content), 0644)
			if err != nil {
				printFatal(err.Error())
			}
		} else {
			// concatenate
			buffer.WriteString(fmt.Sprintf("\n---\n%s\n", generateYamlComment(filename, snapshotUID, formationUID, v.Sequence)))
			buffer.WriteString(v.Content)
		}
	}

	if outdir == "" {
		fmt.Print(buffer.String())
	}
}

func generateYamlComment(filename string, snapshot string, formation string, sequence int) string {
	return fmt.Sprintf("# Stencil: %s\n# Formation: %s\n# Snapshot: %s\n# Sequence: %d\n", filename, formation, snapshot, sequence)
}

func printSnapshotList(w io.Writer, snapshots []cloud66.Snapshot) {
	sort.Sort(snapshotsByDate(snapshots))
	listRec(w,
		"UID",
		"LAST ACTION AT",
		"LAST ACTION BY",
		"ACTION")

	for _, a := range snapshots {
		if a.Uid != "" {
			listSnapshot(w, a)
		}
	}
}

func listSnapshot(w io.Writer, a cloud66.Snapshot) {
	ta := a.TriggeredAt

	listRec(w,
		a.Uid,
		prettyTime{ta},
		a.TriggeredBy,
		a.Action,
	)
}

type snapshotsByDate []cloud66.Snapshot

func (a snapshotsByDate) Len() int      { return len(a) }
func (a snapshotsByDate) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a snapshotsByDate) Less(i, j int) bool {
	return a[i].TriggeredAt.Unix() >= a[j].TriggeredAt.Unix()
}
