package main

import (
	"io"
	"os"
	"sort"
	"strings"

	"text/tabwriter"

	"github.com/mgutz/ansi"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66/cli"
)

var cmdServers = &Command{
	Name:       "servers",
	Build:      buildServers,
	Short:      "commands to work with servers",
	NeedsStack: true,
	NeedsOrg:   false,
}

func buildServers() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		cli.Command{
			Name:   "list",
			Action: runServers,
			Usage:  "lists all the servers of a stack.",
			Description: `List all the servers of a stack.
The information contains the name, IP address, server role and the date/time it was created.
Names can be a list of server names entered as separate parameters.

Examples:
$ cx servers list -s mystack
orca         162.243.201.164  [rails web app]  Healthy   Mar 26 11:23
snowleopard  107.170.98.160   [mysql db]       Building  Mar 26 11:23
$ cx servers list -s mystack orca
orca         162.243.201.164  [rails web app]  Healthy   Mar 26 11:23
`,
		},
		cli.Command{
			Name:   "reboot",
			Action: runServerReboot,
			Usage:  "reboots the specified server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "server",
					Usage: "server name",
				},
			},
			Description: `Reboots a server.

Examples:
$ cx server reboot -s mystack --server lion
`,
		},
		cli.Command{
			Name: "settings",
			Subcommands: []cli.Command{
				cli.Command{
					Name:   "list",
					Action: runServerSettings,
					Usage:  "lists server settings",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
						cli.StringFlag{
							Name:  "environment,e",
							Usage: "full or partial environment name",
						},
						cli.StringFlag{
							Name:  "server",
							Usage: "server name",
						},
					},
					Description: `Lists all the settings applicable to the given server.
It also shows the key, value and the readonly flag for each setting.
Settings can be a list of multiple settings as separate parameters.
To change each server setting, use the server-set command.

Examples:
$ cx servers settings list -s mystack --server lion
server.name         lion                                                       false

$ cx servers settings list -s mystack --server db server.name
server.name         tiger                                                      false
`,
				},
				cli.Command{
					Name:   "set",
					Action: runServerSet,
					Usage:  "sets the value of a setting on a server",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
						cli.StringFlag{
							Name:  "environment,e",
							Usage: "full or partial environment name",
						},
						cli.StringFlag{
							Name:  "server",
							Usage: "server name",
						},
					},
					Description: `This sets and applies the value of a setting on a server. Applying some settings might require more
work and therefore this command will return immediately after the setting operation has started.

Examples:
$ cx servers settings set -s mystack --server lion server.name=tiger
`,
				},
			},
		},
	}

	return base
}

func runServers(c *cli.Context) {
	stack := mustStack(c)
	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()

	var servers []cloud66.Server
	var err error
	servers, err = client.Servers(stack.Uid)
	must(err)

	serverNames := c.Args()

	for idx, i := range serverNames {
		serverNames[idx] = strings.ToLower(i)
	}
	sort.Strings(serverNames)
	if len(serverNames) == 0 {
		printServerList(w, servers)
	} else {
		// filter out the unwanted servers
		var filteredServers []cloud66.Server
		for _, i := range servers {
			sorted := sort.SearchStrings(serverNames, strings.ToLower(i.Name))
			if sorted < len(serverNames) && strings.ToLower(serverNames[sorted]) == strings.ToLower(i.Name) {
				filteredServers = append(filteredServers, i)
			}
		}
		printServerList(w, filteredServers)
	}
}

func printServerList(w io.Writer, servers []cloud66.Server) {
	sort.Sort(serversByName(servers))
	for _, a := range servers {
		if a.Name != "" {
			listServer(w, a)
		}
	}
}

func listServer(w io.Writer, a cloud66.Server) {
	t := a.CreatedAt

	var reboot string
	if a.Notifications["reboot_required"].Value != nil && a.Notifications["reboot_required"].Value.(bool) {
		reboot = ansi.Color("[reboot required]", "red+h")
	}

	listRec(w,
		strings.ToLower(a.Name),
		a.Address,
		a.Roles,
		a.Health(),
		prettyTime{t},
		reboot,
	)
}

type serversByName []cloud66.Server

func (a serversByName) Len() int           { return len(a) }
func (a serversByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a serversByName) Less(i, j int) bool { return a[i].Name < a[j].Name }
