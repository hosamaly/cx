package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/cloud66-oss/cloud66"

	"github.com/cloud66/cli"
)

var cmdRun = &Command{
	Name:  "run",
	Build: buildBasicCommand,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "server,svr",
			Usage: "server on which to run the command [optional]",
		},
		cli.StringFlag{
			Name:  "service,svc",
			Usage: "name of the service in which to run the command [optional - docker/kubernetes stacks only]",
		},
		cli.StringFlag{
			Name:  "container,cnt",
			Usage: "name of the pod/container in which to run the command [optional - docker/kubernetes stacks only]",
		},
		cli.BoolFlag{
			Name:  "interactive,i",
			Usage: "stay in shell (with TTY attached)",
		},
		cli.BoolFlag{
			Name:  "stay",
			Usage: "(deprecated)",
		},
		cli.StringFlag{
			Name:  "shell",
			Usage: "(deprecated)",
		},
	},
	Run:        runRun,
	NeedsStack: true,
	NeedsOrg:   false,
	Short:      "executes a command directly on the server/service/container",
	Long: `This command will execute a command directly on the remote server, in a new service, or in a existing container

For this purpose, this command will open the firewall for SSH from your IP address temporaritly (20 minutes), downloads the keys if you don't have them, starts a SSH session,
and executes the command specified.

If you have docker/kubernetes stack, you can provide additional "service" or "container" arguments. 
If you specify "service" the command will run in a newly created container based on the most recent service image.
If you specify "container" the command will run in context of the existing container.

You need to have the correct access permissions to use this command.
You can use either the server name (ie lion) or the server IP (ie. 123.123.123.123) or the server role (ie. web)
with this command.

If a role is specified the command will connect to the first server with that role.
Names are case insensitive and will work with the starting characters as well.

This command is only supported on Linux and OS X (for Windows you can run this in a virtual machine if necessary)

Examples:
$ cx run -s mystack --server lion 'ls -la' 
(runs "ls -la" ON THE SERVER, returns the output, and exits)

$ cx run -s mystack --server lion -i 
(runs "bash or sh" ON THE SERVER", and remains in the session)

$ cx run -s mystack --svc webapp 'ls -la'
(runs "ls -la" IN A NEW CONTAINER OF THE SERVICE, returns the output, and exits)

$ cx run -s mystack --service api --interactive 'bundle exec rails c'
(runs "bundle exec rails c" IN A NEW CONTAINER OF THE SERVICE, and remains in the session)

$ cx run -s mystack --container web-123 -i 'bundle exec rails c'
(runs "bundle exec rails c" INSIDE THE SPECIFIED CONTAINER, and remains in the session)
`,
}

// ShellCommand is the default command to start a shell
const ShellCommand = "/bin/sh -c 'if [ -e /bin/bash ]; then /bin/bash; else /bin/sh; fi'"

func runRun(c *cli.Context) {
	if runtime.GOOS == "windows" {
		printFatal("Not supported on Windows")
		os.Exit(2)
	}
	serviceName := c.String("service")
	containerName := c.String("container")
	serverName := c.String("server")
	interactive := c.Bool("interactive")
	if serverName == "" && containerName == "" && serviceName == "" {
		printFatal("At least ONE of server/service/container must be specified")
		os.Exit(2)
	}

	userCommand := ""
	if len(c.Args()) > 0 {
		for _, arg := range c.Args() {
			userCommand = fmt.Sprintf("%s %s", userCommand, arg)
		}
	}

	stack := mustStack(c)
	if (serviceName != "" || containerName != "") && stack.Backend != "docker" && stack.Backend != "kubernetes" {
		printFatal("The service & container options only apply to docker/kubernetes stacks")
		os.Exit(2)
	}
	if serviceName != "" && containerName != "" {
		printFatal("Only one of options service OR container may be specified")
		os.Exit(2)
	}

	servers, err := client.Servers(stack.Uid)
	if err != nil {
		printFatal(err.Error())
	}

	var server *cloud66.Server
	if serverName != "" {
		server, err = findServer(servers, serverName)
		if err != nil {
			printFatal(err.Error())
		}
		if server == nil {
			printFatal("Server %s not found", serverName)
		}
	}

	if serviceName == "" && containerName == "" {
		// this is a server level command
		if userCommand == "" && !interactive {
			printFatal("A command is required if you're not running an interactive session")
		}
		err = runServerCommand(*server, userCommand, interactive)
		must(err)
		return
	}

	if stack.Backend == "kubernetes" {
		// we need to run this against the master
		if server == nil {
			for _, stackServer := range servers {
				if stackServer.IsKubernetesMaster {
					server = &stackServer
					break
				}
			}
		}
		if server == nil {
			printFatal("Master server can not be determined")
		}
		if serviceName != "" {
			// this is a service level command
			// we need a session
			asyncResult, err := client.StartRemoteSession(stack.Uid, serviceName)
			must(err)
			genericRes, err := client.WaitStackAsyncAction(asyncResult.Id, stack.Uid, 5*time.Second, 4*time.Minute, false)
			must(err)
			if genericRes.Status != true {
				printFatal("Unable to start session")
			}
			// get that session we've started
			session, err := client.FetchRemoteSession(stack.Uid, nil, &serviceName)
			must(err)
			// now we have pods
			err = runKubesCommand(*server, stack.Namespace(), session.PodName, userCommand, interactive)
			must(err)
		} else if containerName != "" {
			// we have the pod name
			err = runKubesCommand(*server, stack.Namespace(), containerName, userCommand, interactive)
			must(err)
		}
	} else if stack.Backend == "docker" {
		if serviceName != "" {
			if server == nil {
				for _, stackServer := range servers {
					if _, err := fuzzyFind(stackServer.Roles, "docker", true); err == nil {
						server = &stackServer
						break
					}
				}
			}
			// fetch service information for existing server/command
			service, err := client.GetService(stack.Uid, serviceName, &server.Uid, &userCommand)
			must(err)
			userCommand = service.WrapCommand
			if !interactive {
				// we always get interactive back
				userCommand = strings.Replace(userCommand, "-it", "", 1)
			}
		} else if containerName != "" {
			container, err := client.GetContainer(stack.Uid, containerName)
			must(err)
			server, err = findServer(servers, container.ServerName)
			must(err)
			if userCommand == "" {
				userCommand = ShellCommand
			}
			if interactive {
				userCommand = fmt.Sprintf("sudo docker exec -it %s %s", container.Uid, userCommand)
			} else {
				userCommand = fmt.Sprintf("sudo docker exec %s %s", container.Uid, userCommand)
			}
		}
		err = runServerCommand(*server, userCommand, interactive)
		must(err)

	} else {
		printFatal("not supported yet")
	}
}

func runServerCommand(server cloud66.Server, userCommand string, interactive bool) error {
	// open lease, get address, get sshkey
	sshFile, address := prepareForSSH(server)

	// default user command if it isn't specified
	if userCommand == "" {
		userCommand = ShellCommand
	}
	userCommand = fmt.Sprintf("source /var/.cloud66_env &>/dev/null ; %s", userCommand)

	// run the ssh
	return runSSH(address, sshFile, userCommand, interactive)
}

func runKubesCommand(server cloud66.Server, namespace string, podName string, userCommand string, interactive bool) error {
	// open lease, get address, get sshkey
	sshFile, address := prepareForSSH(server)
	// default the command if not supplied
	if userCommand == "" {
		userCommand = ShellCommand
	}
	if interactive {
		// override with shell for interactive
		userCommand = fmt.Sprintf("kubectl --namespace %s exec -it %s -- %s", namespace, podName, userCommand)
	} else {
		userCommand = fmt.Sprintf("kubectl --namespace %s exec %s -- %s", namespace, podName, userCommand)
	}
	// run the ssh
	return runSSH(address, sshFile, userCommand, interactive)
}

func prepareForSSH(server cloud66.Server) (string, string) {
	address := fmt.Sprintf("%s@%s", server.UserName, server.Address)

	sshFile, err := prepareLocalSshKey(server)
	must(err)
	// open the firewall
	var timeToOpen = 2
	genericRes, err := client.LeaseSync(server.StackUid, nil, &timeToOpen, nil, &server.Uid)
	must(err)
	if genericRes.Status != true {
		printFatal("Unable to open server lease")
	}
	return sshFile, address
}

func runSSH(address, sshFile, userCommand string, interactive bool) error {
	if interactive {
		return startProgram("ssh", []string{
			address,
			"-i", sshFile,
			"-o", "UserKnownHostsFile=/dev/null",
			"-o", "CheckHostIP=no",
			"-o", "StrictHostKeyChecking=no",
			"-o", "LogLevel=QUIET",
			"-o", "IdentitiesOnly=yes",
			"-A",
			"-p", "22",
			"-t",
			userCommand,
		})
	}
	return startProgram("ssh", []string{
		address,
		"-i", sshFile,
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "CheckHostIP=no",
		"-o", "StrictHostKeyChecking=no",
		"-o", "LogLevel=QUIET",
		"-o", "IdentitiesOnly=yes",
		"-A",
		"-p", "22",
		userCommand,
	})
}
