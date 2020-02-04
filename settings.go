package main

import (
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66/cli"
)

var cmdSettings = &Command{
	Name:       "settings",
	Build:      buildSettings,
	NeedsStack: true,
	NeedsOrg:   false,
	Short:      "list and set stack settings",
}

func buildSettings() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		cli.Command{
			Name:  "list",
			Usage: "lists all settings",
			Description: `Lists all the settings applicable to the given stack.
It also shows the key, value and the readonly flag for each setting.
Settings can be a list of multiple settings as separate parameters.
To change each setting, use the set command.

Examples:
$ cx settings list -s mystack
git.branch          master                                                     false
git.repository      git://git@github.com:cloud66-samples/rails-psql-redis.git  false
allowed.web.source  <nil>                                                      false

$ cx settings list -s mystack git.branch
git.branch          master                                                     false
`,
			Action: runSettings,
		},
		cli.Command{
			Name:  "set",
			Usage: "sets the value of a setting on a stack",
			Description: `This sets and applies the value of a setting on a stack. Applying some settings might require more
work and therefore this command will return immediately after the setting operation has started.

Examples:
$ cx settings set -s mystack git.branch dev
$ cx settings set -s mystack allowed.web.source 191.203.12.10
$ cx settings set -s mystack allowed.web.source anywhere
$ cx settings set -s mystack maintenance.mode  1|true|on|enable
$ cx settings set -s mystack maintenance.mode  0|false|off|disable
`,
			Action: runSet,
		},
	}

	return base
}

func runSettings(c *cli.Context) {
	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()
	var settings []cloud66.StackSetting
	var err error
	stack := mustStack(c)
	settings, err = client.StackSettings(stack.Uid)
	must(err)

	settingNames := c.Args()
	sort.Strings(settingNames)
	if len(settingNames) == 0 {
		printSettingList(w, settings)
	} else {
		// filter out the unwanted settings
		var filteredSettings []cloud66.StackSetting
		for _, i := range settings {
			sorted := sort.SearchStrings(settingNames, i.Key)
			if sorted < len(settingNames) && settingNames[sorted] == i.Key {
				filteredSettings = append(filteredSettings, i)
			}
		}

		printSettingList(w, filteredSettings)
	}
}

func printSettingList(w io.Writer, settings []cloud66.StackSetting) {
	sort.Sort(settingsByName(settings))
	for _, a := range settings {
		if a.Key != "" {
			listSetting(w, a)
		}
	}
}

func listSetting(w io.Writer, a cloud66.StackSetting) {
	var readonly string
	if a.Readonly {
		readonly = "readonly"
	} else {
		readonly = "read/write"
	}
	listRec(w,
		a.Key,
		a.Value,
		readonly,
	)
}

type settingsByName []cloud66.StackSetting

func (a settingsByName) Len() int           { return len(a) }
func (a settingsByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a settingsByName) Less(i, j int) bool { return a[i].Key < a[j].Key }
