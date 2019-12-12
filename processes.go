package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66/cli"
)

var cmdProcesses = &Command{
	Name:       "processes",
	Build:      buildProcesses,
	Short:      "commands to work with processes",
	NeedsStack: true,
	NeedsOrg:   false,
}

func buildProcesses() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		cli.Command{
			Name:   "list",
			Usage:  "lists all the processes of a stack (or server)",
			Action: runProcesses,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
				cli.StringFlag{
					Name: "name",
				},
			},
			Description: `List all the processes running on a stack or a server.

Examples:
$ cx processes list -s mystack
$ cx processes list -s mystack --server orca
$ cx processes list -s mystack --name worker
$ cx processes list -s mystack --server orca --name worker

Example Output:
NAME       COMMAND                                           SERVER    COUNT
scheduler  bundle exec rake test:work FIRST={{UNIQUE_INT}}   Flamingo  1
scheduler  bundle exec rake test:work FIRST={{UNIQUE_INT}}   Jackal    1
worker     bundle exec rake test:work SECOND={{UNIQUE_INT}}  Flamingo  1
worker     bundle exec rake test:work SECOND={{UNIQUE_INT}}  Jackal    2
`,
		},
		cli.Command{
			Name:   "scale",
			Action: runProcessScale,
			Usage:  "starts and stops processes",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
				cli.StringFlag{
					Name: "name",
				},
			},
			Description: `Starts <count> processes from the given process definition.
<count> can be an absolute value like "2" or a relative value like "[+2]" or "[-3]" etc.
If server is provided, will start <count> processes on that server.
If server is not provided, will start <count> processes on every server.
NOTE: the square brackets are required for relative count values

Examples:
$ cx processes scale -s mystack --server backend1 --name worker [+5]
$ cx processes scale -s mystack --server backend2 --name worker [-5]
$ cx processes scale -s mystack --server backend3 --name worker 15
$ cx processes scale -s mystack --name worker 2`},
		cli.Command{
			Name:   "restart",
			Action: runProcessRestart,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
			},
			Usage: "restarts all processes from the given service and/or server",
			Description: `Restarts all processes from the given service and/or server.

Examples:
$ cx processes restart -s mystack a_backend_process
$ cx processes restart -s mystack --server my_server
$ cx processes restart -s mystack --server my_server a_backend_process
`},
		cli.Command{
			Name:   "pause",
			Action: runProcessPause,
			Usage:  "pauses all processes from the given service and/or server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
			},
			Description: `Pauses all processes from the given service and/or server",

Examples:
$ cx processes pause -s mystack a_backend_process
$ cx processes pause -s mystack --server my_server
$ cx processes pause -s mystack --server my_server a_backend_process
`},
		cli.Command{
			Name:   "resume",
			Action: runProcessResume,
			Usage:  "resumes all paused processes from the given service and/or server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
			},
			Description: `Resumes all paused processes from the given service and/or server",

Examples:
$ cx processes resume -s mystack a_backend_process
$ cx processes resume -s mystack --server my_server
$ cx processes resume -s mystack --server my_server a_backend_process
`},
	}

	return base
}

func runProcesses(c *cli.Context) {
	flagServer := c.String("server")
	flagName := c.String("name")

	stack := mustStack(c)
	w := tabwriter.NewWriter(os.Stdout, 1, 2, 2, ' ', 0)
	defer w.Flush()

	var serverUid *string
	if flagServer == "" {
		serverUid = nil
	} else {
		servers, err := client.Servers(stack.Uid)
		if err != nil {
			printFatal(err.Error())
		}
		server, err := findServer(servers, flagServer)
		if err != nil {
			printFatal(err.Error())
		}
		if server == nil {
			printFatal("Server '" + flagServer + "' not found")
		}
		if !server.HasRole("app") || server.HasRole("docker") || server.HasRole("kubes") {
			printFatal("Server '" + flagServer + "' can not host processes")
		}
		fmt.Printf("Server: %s\n", server.Name)
		serverUid = &server.Uid
	}

	var (
		processes []cloud66.Process
		err       error
	)

	if flagName == "" {
		processes, err = client.GetProcesses(stack.Uid, serverUid)
		must(err)
	} else {
		fmt.Printf("Process: %s\n", flagName)
		process, err := client.GetProcess(stack.Uid, flagName, serverUid)
		must(err)
		if process == nil {
			printFatal("Process '" + flagName + "' not found on specified stack")
		} else {
			processes = make([]cloud66.Process, 1)
			processes[0] = *process
		}
	}
	printProcessesList(w, processes)
}

func printProcessesList(w io.Writer, processes []cloud66.Process) {
	listRec(w,
		"NAME",
		"COMMAND",
		"SERVER",
		"COUNT",
	)

	sort.Sort(ProcessByNameServer(processes))
	for _, a := range processes {
		listProcess(w, a)
	}
}

func listProcess(w io.Writer, process cloud66.Process) {
	var serverNames []string
	for serverName := range process.ServerProcessCount {
		serverNames = append(serverNames, serverName)
	}
	sort.Strings(serverNames)
	for _, serverName := range serverNames {
		count := process.ServerProcessCount[serverName]
		listRec(w,
			process.Name,
			process.Command,
			serverName,
			count,
		)
	}
}

func startProcessAction(stackUid string, processName *string, serverUid *string, action string) (*int, error) {
	asyncRes, err := client.InvokeProcessAction(stackUid, processName, serverUid, action)
	if err != nil {
		return nil, err
	}
	return &asyncRes.Id, err
}

func endProcessAction(asyncId int, stackUid string) (*cloud66.GenericResponse, error) {
	return client.WaitStackAsyncAction(asyncId, stackUid, 5*time.Second, 10*time.Minute, true)
}

type ProcessByNameServer []cloud66.Process

func (a ProcessByNameServer) Len() int           { return len(a) }
func (a ProcessByNameServer) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ProcessByNameServer) Less(i, j int) bool { return a[i].Name < a[j].Name }
