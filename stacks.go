package main

import (
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66/cli"
)

var cmdStacks = &Command{
	Name:  "stacks",
	Build: buildStacks,
	Short: "commands to work with stacks",
}

func buildStacks() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		cli.Command{
			Name:  "list",
			Usage: "lists all stacks",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "environment,e",
					Usage: "full or partial environment name",
				},
				cli.StringFlag{
					Name:  "output,o",
					Usage: "tailor output view (standard|wide)",
				},
			},
			Action: runStacks,
			Description: `Lists stacks. Shows the stack name, environment, and last deploy time.
You can use multiple names at the same time.

Examples:
$ cx stacks list
mystack     production   Jan 2 12:34
mystack     staging      Feb 2 12:34
mystack-2   development  Jan 2 12:35

$ cx stacks list mystack-2 
mystack-2   development  Jan 2 12:34

$ cx stacks list mystack -e staging -o wide

`,
		},
		cli.Command{
			Name:  "create",
			Usage: "creates a new Maestro stack",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "name,n",
					Usage: "Stack name.",
				},
				cli.StringFlag{
					Name:  "service_yaml,s",
					Usage: "File containing your service definition.",
				},
				cli.StringFlag{
					Name:  "manifest_yaml,m",
					Usage: "File containing your manifest definition (optional)",
				},
				cli.StringFlag{
					Name:  "environment,e",
					Usage: "[deprecated]",
				},
			},
			Action: runCreateStack,
			Description: `Creates a new docker stack.

Examples:
$ cx stacks create --name my_maestro_stack --service_yaml service.yml --manifest_yaml manifest.yml
`,
		},
		cli.Command{
			Name:  "redeploy",
			Usage: "redeploys a stack",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "y",
					Usage: "answer yes to confirmations",
				},
				cli.BoolFlag{
					Name:  "listen",
					Usage: "waits for deployment to complete, shows progress and log output when available",
				},
				cli.StringFlag{
					Name:  "git-ref",
					Usage: "[classic stacks] git reference",
				},
				cli.StringSliceFlag{
					Name:  "service",
					Usage: "[docker stacks] service name (and optional colon separated reference) to include in the deploy. Repeatable for multiple services",
					Value: &cli.StringSlice{},
				},
				cli.StringFlag{
					Name:  "deploy-strategy",
					Usage: "override your stack settings and/or deployment profile settings, and use this deployment strategy instead. Options are serial, parallel, rolling (rails only) or fast (maestro only)",
				},
				cli.StringFlag{
					Name:  "environment,e",
					Usage: "full or partial environment name",
				},
				cli.StringFlag{
					Name:  "stack,s",
					Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
				},
			},
			Action: runRedeploy,
			Description: `Enqueues redeployment of the stack. If the stack is already building, another build will be enqueued and performed immediately after the current one is finished.
			
   -y answers yes to confirmation question if the stack is production.
   --git-ref will redeploy the specific branch, tag or hash git reference [classic stacks]
   --service is a repeateable option to deploy only the specified service(s). Including a reference (separated by a colon) will attempt to deploy that particular reference for that service [docker stacks]
   --deploy-strategy is an override for the deploy strategy you want to use. Options are serial, parallel, rolling (rails only) or fast (maestro only)	
`,
		},
		cli.Command{
			Name:   "restart",
			Action: runRestart,
			Flags:  basicFlags(),
			Usage:  "restarts all components of a stack",
			Description: `This will send a restart method to all stack components. This means different things for different components.
For a web server, it means a restart of nginx. For an application server, this might be a restart of the workers like Unicorn.
For more information on restart command, please refer to help.cloud66.com
`,
		},
		cli.Command{
			Name:   "reboot",
			Action: runStackReboot,
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "y",
					Usage: "answer yes to confirmations",
				},
				cli.StringFlag{
					Name:  "group",
					Usage: "Specify which group you would like to reboot",
				},
				cli.StringFlag{
					Name:  "strategy",
					Usage: "Specify how you would like to reboot your servers",
				},
				cli.StringFlag{
					Name:  "environment,e",
					Usage: "full or partial environment name",
				},
				cli.StringFlag{
					Name:  "stack,s",
					Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
				},
			},
			Usage: "reboot servers in your stack",
			Description: `reboot servers in your stack.

The group parameter specifies which group of servers you wish to reboot. Valid values are "all", "web", "haproxy", "db";
DB specific values like "mysql" or "redis" for example are also supported.
If this value is left unspecified, the default value of "web" will be used

The strategy parameter specifies whether you want all your servers to be rebooted in parallel or in serial.
Valid values for this parameter are "serial" or "parallel"; "serial" reboots involves web servers being removed/re-added to the LB one by one.
Note that for this only applies to web servers; non-web server will still be rebooted in parallel.
If this value is left unspecified, Cloud 66 will determine the best strategy based on your infrastructure layout.

Examples:
$ cx stack reboot -s mystack
$ cx stack reboot -s mystack --group web
$ cx stack reboot -s mystack --group all
$ cx stack reboot -s mystack --strategy parallel
$ cx stack reboot -s mystack --group web --strategy serial 
`},

		cli.Command{
			Name:   "clear-caches",
			Action: runClearCaches,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "environment,e",
					Usage: "full or partial environment name",
				},
				cli.StringFlag{
					Name:  "stack,s",
					Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
				},
			},
			Usage: "clears all existing stack code caches",
			Description: `Clears all existing code caches.

For improved performance, volatile code caches exist for your stack.
It is possible for a those volatile caches to become invalid if you switch branches, change git repository URL, or rebase or force a commit.
Since switching branch or changing git repository URL is done via the Cloud 66 interface, your volatile caches will automatically be purged.
However, rebasing or forcing a commit doesn't have any association with Cloud 66, so this command can be used to purge the exising volatile caches.
`},
		cli.Command{
			Name:   "listen",
			Action: runListen,
			Usage:  "tails all deployment logs",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "environment,e",
					Usage: "full or partial environment name",
				},
				cli.StringFlag{
					Name:  "stack,s",
					Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
				},
			},
			Description: `This acts as a log tail for deployment of a stack so you don't have to follow the deployment on the web.

Examples:
$ cx stacks listen
$ cx stacks listen -s mystack
`},
		cli.Command{
			Name:  "configure",
			Usage: "list, download & upload of service.yml & manifest.yml files (NOTE: eventually replaced by the \"configuration\" command)",
			Subcommands: []cli.Command{
				cli.Command{
					Name:   "list-versions",
					Action: runStackConfigureFileListVersions,
					Usage:  "list of all versions of a configuration file",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "file,f",
							Usage: "supported values are: service.yml , manifest.yml",
						},
						cli.StringFlag{
							Name:  "environment,e",
							Usage: "full or partial environment name",
						},
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
					},
					Description: `This acts list of all versions of configuration file.
`},
				cli.Command{
					Name:   "download",
					Action: runStackConfigureDownloadFile,
					Usage:  "download a configuration file",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "file,f",
							Usage: "supported values are: service.yml , manifest.yml",
						},
						cli.StringFlag{
							Name:  "version,v",
							Usage: "full or partial file version (optional)",
						},
						cli.StringFlag{
							Name:  "output,o",
							Usage: "full path of output file (optional)",
						},
						cli.StringFlag{
							Name:  "environment,e",
							Usage: "full or partial environment name",
						},
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
					},
					Description: `download service.yml and manifest.yml.
`},
				cli.Command{
					Name:   "upload",
					Action: runStackConfigureUploadFile,
					Usage:  "uploading new version of configuration file",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "file,f",
							Usage: "supported values are: service.yml , manifest.yml",
						},
						cli.StringFlag{
							Name:  "comments,c",
							Usage: "a brief description of your changes",
						},
						cli.StringFlag{
							Name:  "environment,e",
							Usage: "full or partial environment name",
						},
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
					},
					Description: `upload new service.yml or manifest.yml.
`},
			},
			Description: `

Examples:
$ cx stacks configure list -f service.yml -s mystack
$ cx stacks configure download -f manifest.yml -s mystack
$ cx stacks configure download -f service.yml -o /tmp/my_stack_servive.yml -s mystack
$ cx stacks configure download -f manifest.yml -v f345 -s mystack
$ cx stacks configure upload /tmp/mystack_edited_service.yml -f service.yml -s mystack --comments "new service added"
`},
		cli.Command{
			Name:  "configuration",
			Usage: "list, download, upload & apply configuration files (NOTE: eventually replacing the \"configure\" command)",
			Subcommands: []cli.Command{
				cli.Command{
					Name:   "list",
					Action: runStackConfigurationList,
					Usage:  "list of all configuration files available for this stack",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "environment,e",
							Usage: "full or partial environment name",
						},
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
					},
					Description: `This act lists all configuration files available on the stack.
`},
				cli.Command{
					Name:   "download",
					Action: runStackConfigurationDownload,
					Usage:  "gets the content of the specified configuration type on the stack",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "type,t",
							Usage: "type of the configuration file (see `list` for types available on your stack)",
						},
						cli.StringFlag{
							Name:  "output,o",
							Usage: "save configuration output to a file (optional, default is stdout)",
						},

						cli.StringFlag{
							Name:  "environment,e",
							Usage: "full or partial environment name",
						},
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
					},
					Description: `gets the content of the specified configuration type on the stack
`},
				cli.Command{
					Name:   "upload",
					Action: runStackConfigurationUpload,
					Usage:  "sets the content of the specified configuration type on the stack (optionally applies it)",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "type,t",
							Usage: "type of the configuration file (see `list` for types available on your stack)",
						},
						cli.StringFlag{
							Name:  "source",
							Usage: "the source file containing the configuration you wish to push up to your stack",
						},
						cli.BoolFlag{
							Name:  "no-apply",
							Usage: "do not automatically apply the configuration changes to your servers (default behaviour is to apply changes immediately)",
						},
						cli.StringFlag{
							Name:  "commit-message",
							Usage: "a message to associate with the configuration update (optional)",
						},
						cli.StringFlag{
							Name:  "environment,e",
							Usage: "full or partial environment name",
						},
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
					},
					Description: `sets the content of the specified configuration type on the stack
`},
				cli.Command{
					Name:   "apply",
					Action: runStackConfigurationApply,
					Usage:  "apply the specified configuration type to the stack servers",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "type,t",
							Usage: "type of the configuration file (see `list` for types available on your stack)",
						},
						cli.StringFlag{
							Name:  "environment,e",
							Usage: "full or partial environment name",
						},
						cli.StringFlag{
							Name:  "stack,s",
							Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
						},
					},
					Description: `apply the specified configuration type on the stack
`},
			},
			Description: `

Examples:
$ cx stacks configuration list -s mystack
`},
		buildStacksSSL(),
	}

	return base
}

func buildStackFlag() cli.StringFlag {
	return cli.StringFlag{
		Name:  "stack,s",
		Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
	}
}

func runStacks(c *cli.Context) {
	names := c.Args()
	environment := c.String("environment")
	output := c.String("output")
	if output == "" {
		output = "standard"
	}
	listStacks(false, names, environment, output)
}

func listStacks(showClusters bool, names []string, environment, output string) {
	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()
	var stacks []cloud66.Stack
	if len(names) == 0 {
		var err error
		stacks, err = client.StackListWithFilter(func(item interface{}) bool {
			if environment == "" {
				return true
			}
			return strings.HasPrefix(strings.ToLower(item.(cloud66.Stack).Environment), strings.ToLower(environment))
		})
		must(err)
	} else {
		stackch := make(chan *cloud66.Stack, len(names))
		errch := make(chan error, len(names))
		for _, name := range names {
			if name == "" {
				stackch <- nil
			} else {
				go func(stackname string) {
					if stack, err := client.StackInfoWithEnvironment(stackname, environment); err != nil {
						errch <- err
					} else {
						stackch <- stack
					}
				}(name)
			}
		}
		for range names {
			select {
			case err := <-errch:
				printFatal(err.Error())
			case stack := <-stackch:
				if stack != nil {
					stacks = append(stacks, *stack)
				}
			}
		}
	}
	printStackList(w, stacks, output)
}

func printStackList(w io.Writer, stacks []cloud66.Stack, output string) {
	if output == "wide" {
		listRec(w,
			"ACCOUNT",
			"NAME",
			"ENVIRONMENT",
			"STACK TYPE",
			"CLUSTER NAME",
			"APPLICATION ADDRESS",
			"STATUS",
			"LAST ACTIVITY")
	} else {
		listRec(w,
			"NAME",
			"ENVIRONMENT",
			"STACK TYPE",
			"STATUS",
			"LAST ACTIVITY")
	}
	sort.Sort(stacksByAccountThenName(stacks))
	for _, stack := range stacks {
		if stack.Name != "" {
			listStack(w, stack, output)
		}
	}
}

func listStack(w io.Writer, stack cloud66.Stack, output string) {
	t := stack.CreatedAt
	if stack.LastActivity != nil {
		t = *stack.LastActivity
	}
	var stackType string
	clusterName := "n/a"
	environment := stack.Environment
	if environment == "" {
		environment = "n/a"
	}
	if stack.IsCluster {
		environment = "n/a"
		stackType = "kubernetes/cluster"
	} else if stack.IsInsideCluster {
		clusterName = stack.ClusterName
		stackType = "kubernetes/in-cluster"
	} else {
		if stack.Framework == "skycap" {
			environment = "n/a"
			stackType = "skycap"
		} else {
			if stack.Backend == "docker" {
				stackType = "docker"
			} else if stack.Backend == "kubernetes" {
				stackType = "kubernetes/standalone"
			} else {
				stackType = "ruby/rack"
			}
		}
	}

	applicationAddress := "n/a"
	if stack.ApplicationAddress != nil {
		applicationAddress = *stack.ApplicationAddress
	}

	if output == "wide" {
		listRec(w,
			stack.AccountName,
			stack.Name,
			environment,
			stackType,
			clusterName,
			applicationAddress,
			stack.Status(),
			prettyTime{t},
		)
	} else {
		listRec(w,
			stack.Name,
			environment,
			stackType,
			stack.Status(),
			prettyTime{t},
		)
	}
}

func basicFlags() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:  "environment,e",
			Usage: "full or partial environment name",
		},
		cli.StringFlag{
			Name:  "stack,s",
			Usage: "full or partial stack name. This can be omitted if the current directory is a stack directory",
		},
	}
}

type stacksByAccountThenName []cloud66.Stack

func (arr stacksByAccountThenName) Len() int      { return len(arr) }
func (arr stacksByAccountThenName) Swap(i, j int) { arr[i], arr[j] = arr[j], arr[i] }
func (arr stacksByAccountThenName) Less(i, j int) bool {
	accName1 := strings.ToLower(arr[i].AccountName)
	accName2 := strings.ToLower(arr[j].AccountName)
	name1 := strings.ToLower(arr[i].Name)
	name2 := strings.ToLower(arr[j].Name)
	return accName1 < accName2 || (accName1 < accName2 && name1 < name2)
}
