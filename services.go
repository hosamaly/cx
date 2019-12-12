package main

import (
	"fmt"
	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66/cli"
	"io"
	"os"
	"sort"
	"text/tabwriter"
	"time"
)

var cmdServices = &Command{
	Name:       "services",
	Build:      buildServices,
	Short:      "commands to work with services",
	NeedsStack: true,
	NeedsOrg:   false,
}

func buildServices() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		cli.Command{
			Name:   "list",
			Usage:  "lists all the services of a stack (or server)",
			Action: runServices,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
				cli.StringFlag{
					Name: "service",
				},
			},
			Description: `List all the services and running containers of a stack or a server.

Examples:
$ cx services list -s mystack
$ cx services list -s mystack --server orca
$ cx services list -s mystack --server orca --service web
$ cx services list -s mystack --service web
`,
		},
		cli.Command{
			Name:   "stop",
			Action: runServiceStop,
			Usage:  "stops all the containers from the given service",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
			},
			Description: `Stops all the containers from the given service.
The list of available stack services can be obtained through the 'services' command.
If the server is provided it will only act on the specified server.

Examples:
$ cx services stop -s mystack my_web_service
$ cx services stop -s mystack a_backend_service
$ cx services stop -s mystack --server my_server my_web_service
`},
		cli.Command{
			Name:   "pause",
			Action: runServicePause,
			Usage:  "pauses all the containers from the given service",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
			},
			Description: `Pauses all the containers from the given service.
The list of available stack services can be obtained through the 'services' command.
If the server is provided it will only act on the specified server.

Examples:
$ cx services pause -s mystack my_web_service
$ cx services pause -s mystack a_backend_service
$ cx services pause -s mystack --server my_server my_web_service
`},
		cli.Command{
			Name:   "resume",
			Action: runServiceResume,
			Usage:  "resumes all the containers from the given service that were previously paused",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
			},
			Description: `Resumes all the containers from the given service that were previously paused.
The list of available stack services can be obtained through the 'services' command.
If the server is provided it will only act on the specified server.

Examples:
$ cx services pause -s mystack my_web_service
$ cx services pause -s mystack a_backend_service
$ cx services pause -s mystack --server my_server my_web_service
`},
		cli.Command{
			Name:   "scale",
			Action: runServiceScale,
			Usage:  "starts containers from the given service",
			Flags:  []cli.Flag{},
			Description: `Starts <count> containers of the given service across the stack.
   If <count> is an absolute value like "2", then there will be a total of <count> containers across the stack.
   If <count> is a relative value like "[+2]" or "[-3]", then the current total count of containers across the stack will be changed by <count>.
   NOTE: the square brackets are required for relative count values.

Examples:
   $ cx services scale -s mystack my_web_service 1
   $ cx services scale -s mystack a_backend_service [+5]
   $ cx services scale -s mystack a_backend_service [-2]
`},
		cli.Command{
			Name:   "restart",
			Action: runServiceRestart,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
			},
			Usage: "restarts all the containers from the given service",
			Description: `Restarts all the containers from the given service.
The list of available stack services can be obtained through the 'services' command.
If the server is provided it will only act on the specified server.

Examples:
$ cx services restart -s mystack my_web_service
$ cx services restart -s mystack a_backend_service
$ cx services restart -s mystack --server my_server my_web_service
`},
		cli.Command{
			Name:   "info",
			Action: runServiceInfo,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "server",
				},
			},
			Usage: "get info from given the service`",
			Description: `Get info from given the service.
The list of available stack services can be obtained through the 'services' command.
If the server is provided it will only act on the specified server.

Examples:
$ cx services info -s mystack my_web_service
$ cx services info -s mystack a_backend_service
$ cx services info -s mystack --server my_server my_web_service
`},
	}

	return base
}

func runServices(c *cli.Context) {
	flagServer := c.String("server")
	flagServiceName := c.String("service")
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
		if !server.HasRole("docker") && !server.HasRole("kubes") {
			printFatal("Server '" + flagServer + "' can not host containers")
		}
		fmt.Printf("Server: %s\n", server.Name)
		serverUid = &server.Uid
	}

	var (
		services []cloud66.Service
		err      error
	)

	if flagServiceName == "" {
		services, err = client.GetServices(stack.Uid, serverUid)
		must(err)
	} else {
		service, err := client.GetService(stack.Uid, flagServiceName, serverUid, nil)
		must(err)
		if service == nil {
			printFatal("Service '" + flagServiceName + "' not found on specified stack")
		} else {
			services = make([]cloud66.Service, 1)
			services[0] = *service
		}
	}
	printServicesList(w, services, flagServer)
}

func printServicesList(w io.Writer, services []cloud66.Service, flagServer string) {
	listRec(w,
		"SERVICE NAME",
		"SERVER",
		"COUNT",
	)

	sort.Sort(ServiceByNameServer(services))
	for _, a := range services {
		listService(w, a, flagServer)
	}
}

func listService(w io.Writer, a cloud66.Service, flagServer string) {
	if len(a.Containers) != 0 {
		for serverName, count := range a.ServerContainerCountMap() {
			listRec(w,
				a.Name,
				serverName,
				count,
			)
		}
	} else if flagServer == "" {
		listRec(w,
			a.Name,
			"n/a",
			"0",
		)
	}

}

func startServiceAction(stackUid string, serviceName *string, serverUid *string, action string) (*int, error) {
	asyncRes, err := client.InvokeServiceAction(stackUid, serviceName, serverUid, action)
	if err != nil {
		return nil, err
	}
	return &asyncRes.Id, err
}

func endServiceAction(asyncId int, stackUid string) (*cloud66.GenericResponse, error) {
	return client.WaitStackAsyncAction(asyncId, stackUid, 5*time.Second, 10*time.Minute, true)
}

type ServiceByNameServer []cloud66.Service

func (a ServiceByNameServer) Len() int           { return len(a) }
func (a ServiceByNameServer) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ServiceByNameServer) Less(i, j int) bool { return a[i].Name < a[j].Name }
