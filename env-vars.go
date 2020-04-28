package main

import (
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/cloud66-oss/cloud66"

	"github.com/cloud66/cli"
)

var cmdEnvVars = &Command{
	Name:       "env-vars",
	Build:      buildEnvVars,
	Run:        runEnvVars,
	Short:      "commands to work with environment variables",
	NeedsStack: true,
	NeedsOrg:   false,
}

func buildEnvVars() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		{
			Name:   "list",
			Usage:  "lists environment variables",
			Action: runEnvVars,
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "history",
					Usage: "show environment variable history",
				},
			},
			Description: `Lists all the environment variables of the given stack.
The environment_variables options can be a list of multiple environment_variables as separate parameters.
To change environment variable values, use the env-vars set command.

Examples:
$ cx env-vars list -s mystack
RAILS_ENV 			production
STACK_BASE      	/abc/def
STACK_PATH      	/abc/def/current
etc..

$ cx env-vars list -s mystack RAILS_ENV
RAILS_ENV 			production

$ cx env-vars list -s mystack RAILS_ENV STACK_BASE
RAILS_ENV 			production
STACK_BASE      	/abc/def

$ cx env-vars list -s mystack -history
RAILS_ENV 			production
STACK_BASE      	/abc/def
--> 2015-02-24 12:32:11     /xyz/123
--> 2015-03-12 15:54:08     /xyz/456
STACK_PATH      	/abc/def/current

$ cx env-vars list -s mystack -history STACK_BASE
STACK_BASE      	/abc/def
--> 2015-02-24 12:32:11     /xyz/123
--> 2015-03-12 15:54:08     /xyz/456
`,
		},
		{
			Name:   "set",
			Usage:  "sets the value of an environment variable on a stack",
			Action: runEnvVarsSet,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "apply-strategy",
					Usage: "apply changes immediately, or during next deployment",
				},
			},
			Description: `This sets and applies the value of an environment variable on a stack.
This work happens in the background, therefore this command will return immediately after the operation has started.

You can use the apply-strategy option to specify "immediately" or "deployment". This will determine how Cloud 66 will apply
these environment variables to your servers. The default is "immediately" (for backwards compatibility) 
			
Warning! Applying environment variable changes "immediately" will result in all your environment variables
being sent to your servers immediately, and running processes being restarted. NOTE: If you have load balancer, we will
automatically remove servers from the load balancer before applying changes.
			
Examples:
$ cx env-vars set -s mystack FIRST_VAR=123
$ cx env-vars set -s mystack SECOND_ONE='this value has a space in it'
$ cx env-vars set -s mystack --apply-strategy=immediately EXAMPLE1='this will be applied on immediately' 
$ cx env-vars set -s mystack --apply-strategy=deployment EXAMPLE2='this will be applied on next deployment'
`,
		},
	}

	return base
}

func runEnvVars(c *cli.Context) {
	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()
	var envVars []cloud66.StackEnvVar
	var err error
	stack := mustStack(c)
	envVars, err = client.StackEnvVars(stack.Uid)
	must(err)

	envVarKeys := c.Args()
	flagShowHistory := c.Bool("history")

	sort.Strings(envVarKeys)
	if len(envVarKeys) == 0 {
		printEnvVarsList(w, envVars, flagShowHistory)
	} else {
		// filter out the unwanted env_vars
		var filteredEnvVars []cloud66.StackEnvVar
		for _, i := range envVars {
			sorted := sort.SearchStrings(envVarKeys, i.Key)
			if sorted < len(envVarKeys) && envVarKeys[sorted] == i.Key {
				filteredEnvVars = append(filteredEnvVars, i)
			}
		}
		printEnvVarsList(w, filteredEnvVars, flagShowHistory)
	}
}

func printEnvVarsList(w io.Writer, envVars []cloud66.StackEnvVar, showHistory bool) {
	sort.Sort(envVarsByName(envVars))
	for _, a := range envVars {
		if a.Key != "" {
			listEnvVar(w, a, showHistory)
		}
	}
}

func listEnvVar(w io.Writer, a cloud66.StackEnvVar, showHistory bool) {
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

	if showHistory {
		for _, h := range a.History {
			listRec(w, "----->", h.Value, h.UpdatedAt)
		}
	}
}

type envVarsByName []cloud66.StackEnvVar

func (a envVarsByName) Len() int      { return len(a) }
func (a envVarsByName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a envVarsByName) Less(i, j int) bool {
	if a[i].Readonly == a[j].Readonly {
		return a[i].Key < a[j].Key
	}
	return boolToInt(a[i].Readonly) > boolToInt(a[j].Readonly)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
