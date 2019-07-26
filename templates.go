package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66/cli"
)

var cmdTemplates = &Command{
	Name:       "templates",
	Build:      buildTemplates,
	NeedsStack: false,
	NeedsOrg:   true,
	Short:      "stencil template repository management",
}

func buildTemplates() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		cli.Command{
			Name:   "list",
			Usage:  "shows the list of all stencil template repositories in an account",
			Action: runListTemplates,
			Description: `

Examples:
$ cx templates list
Name                          Short Name 				Uid                                  Git Repository                                           Git Branch  Status
First Awesome Repository      first-awesome-repo		bt-2e0810a17c33ab35d7970ff330b1f916  git@github.com:AwesomeOrganization/awesome-stencils.git  test        Available
Second Awesome Repository     second-awesome-repo 		bt-e2e869ee6ce97ee58a17aa264bed1e0c  git@github.com:AwesomeOrganization/better-stencils.git   test        Available
`,
		},
		cli.Command{
			Name:   "show",
			Usage:  "shows a single template repository and its stencils",
			Action: runShowBTR,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "name,n",
					Usage: "name of the template repository",
				},
			},
		},
		cli.Command{
			Name:  "resync",
			Usage: "pulls the latest code from the stencil template repository",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "template,t",
					Usage: "template UID",
				},
			},
			Action: runResyncTemplate,
			Description: `

Examples:
$ cx templates resync --template='bt-2e0810a17c33ab35d7970ff330b1f916'
`,
		},
	}

	return base
}

func runListTemplates(c *cli.Context) {
	mustOrg(c)

	baseTemplates, err := client.ListBaseTemplates()
	if err != nil {
		printFatal(err.Error())
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()

	printBaseTemplates(w, baseTemplates)
}

func runShowBTR(c *cli.Context) {
	mustOrg(c)

	btrName := c.String("name")
	if btrName == "" {
		printFatal("No base template name given. Please use --name")
	}

	btrs, err := client.ListBaseTemplates()
	if err != nil {
		printFatal(err.Error())
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()

	for _, btr := range btrs {
		if btr.Name == btrName {
			fullBTR, err := client.GetBaseTemplate(btr.Uid, true, false)
			if err != nil {
				printFatal(err.Error())
			}

			printStencilTemplateList(w, fullBTR.Stencils)
		}
	}
}

func runResyncTemplate(c *cli.Context) {
	mustOrg(c)

	baseTemplateUID := c.String("template")
	if baseTemplateUID == "" {
		printFatal("No template UID specified. Please use the --template flag to specify one.")
	}

	baseTemplates, err := client.ListBaseTemplates()
	if err != nil {
		printFatal(err.Error())
	}

	requestedBaseTemplateIndex, err := getBaseTemplateIndexByUID(baseTemplates, baseTemplateUID)
	if err != nil {
		printFatal(err.Error())
	}

	baseTemplate, err := client.SyncBaseTemplate(baseTemplates[requestedBaseTemplateIndex].Uid)
	if err != nil {
		printFatal(err.Error())
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()

	printBaseTemplate(w, baseTemplate)
}

func printBaseTemplates(w io.Writer, baseTemplates []cloud66.BaseTemplate) {
	printBaseTemplateHeader(w)
	for _, baseTemplate := range baseTemplates {
		printBaseTemplateRow(w, &baseTemplate)
	}
}

func printBaseTemplate(w io.Writer, baseTemplate *cloud66.BaseTemplate) {
	printBaseTemplateHeader(w)
	printBaseTemplateRow(w, baseTemplate)
}

func printBaseTemplateHeader(w io.Writer) {
	listRec(w,
		"Name",
		"ShortName",
		"Uid",
		"Git Repository",
		"Git Branch",
		"Status",
	)
}

func printBaseTemplateRow(w io.Writer, baseTemplate *cloud66.BaseTemplate) {
	listRec(w,
		baseTemplate.Name,
		baseTemplate.ShortName,
		baseTemplate.Uid,
		baseTemplate.GitRepo,
		baseTemplate.GitBranch,
		baseTemplate.Status(),
	)
}

func getBaseTemplateIndexByUID(baseTemplates []cloud66.BaseTemplate, baseTemplateUID string) (int, error) {
	for i, baseTemplate := range baseTemplates {
		if baseTemplate.Uid == baseTemplateUID {
			return i, nil
		}
	}

	return -1, fmt.Errorf("Could not find template repository with UID %s.", baseTemplateUID)
}

func listStencilTemplate(w io.Writer, a cloud66.StencilTemplate) {
	listRec(w,
		a.Filename,
		a.Name,
		a.FilenamePattern,
		a.Description,
		a.ContextType,
		a.Tags,
		a.PreferredSequence,
	)
}

func printStencilTemplateList(w io.Writer, stencils []cloud66.StencilTemplate) {
	sort.Sort(stencilTemplateByFilename(stencils))

	listRec(w,
		"FILENAME",
		"NAME",
		"PATTERN",
		"DESCRIPTION",
		"CONTEXT TYPE",
		"TAGS",
		"PREFERRED SEQUENCE")

	for _, a := range stencils {
		if a.Name != "" {
			listStencilTemplate(w, a)
		}
	}
}

type stencilTemplateByFilename []cloud66.StencilTemplate

func (a stencilTemplateByFilename) Len() int           { return len(a) }
func (a stencilTemplateByFilename) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a stencilTemplateByFilename) Less(i, j int) bool { return a[i].Filename < a[j].Filename }
