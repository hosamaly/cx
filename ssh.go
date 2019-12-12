package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66/cli"
)

var cmdSsh = &Command{
	Name:  "ssh",
	Run:   runSsh,
	Build: buildBasicCommand,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "gateway-key",
			Usage: "path to the bastion server key",
			Value: "",
		},
		cli.BoolFlag{
			Name:  "v",
			Usage: "run ssh with verbose flag",
		},
		cli.BoolFlag{
			Name:  "vv",
			Usage: "run ssh with very verbose flag",
		},
		cli.BoolFlag{
			Name:  "vvv",
			Usage: "run ssh with very very verbose flag",
		},
	},
	NeedsStack: true,
	NeedsOrg:   false,
	Short:      "starts a ssh shell into the server",
	Long: `This will open the firewall for SSH from your IP address temporaritly (20 minutes), downloads the keys if you don't have them
and starts a SSH session.

You need to have the right access permissions to use this command.
You can use either the server name (ie lion) or the server IP (ie. 123.123.123.123) or the server role (ie. web)
with thie command.

If a role is specified the command will connect to the first server with that role.

Names are case insensitive and will work with the starting characters as well.

You should provide a key to your bastion server if it is deployed with a deploy gateway.

This command is only supported on Linux and OS X.

Examples:
$ cx ssh -s mystack lion
$ cx ssh -s mystack 52.65.34.98
$ cx ssh -s mystack web
$ cx ssh --gateway-key ~/.ssh/bastion_key  -s mystack db
`,
}

func runSsh(c *cli.Context) {
	if runtime.GOOS == "windows" {
		printFatal("Not supported on Windows")
		os.Exit(2)
	}

	stack := mustStack(c)

	if len(c.Args()) != 1 {
		cli.ShowCommandHelp(c, "ssh")
		os.Exit(2)
	}

	// get the server
	serverName := c.Args()[0]

	servers, err := client.Servers(stack.Uid)
	if err != nil {
		printFatal(err.Error())
	}

	server, err := findServer(servers, serverName)
	if err != nil {
		printFatal(err.Error())
	}

	if server == nil {
		printFatal("Server '" + serverName + "' not found")
	}

	flagGatewayKey := ""
	if server.HasDeployGateway {
		if c.IsSet("gateway-key") {
			flagGatewayKey = c.String("gateway-key")
			if len(server.DeployGatewayAddress) == 0 {
				printFatal("Can not find the address of gateway server")
			}
			if len(server.DeployGatewayUsername) == 0 {
				printFatal("Can not find the username of gateway server")
			}
		} else {
			cli.ShowCommandHelp(c, "ssh")
			printFatal("This server deployed behind the gateway. You need to specify the key for the bastion server")
		}
	}

	verbosity := 0
	if c.Bool("v") {
		verbosity = 1
	} else if c.Bool("vv") {
		verbosity = 2
	} else if c.Bool("vvv") {
		verbosity = 3
	}

	fmt.Printf("Server: %s\n", server.Name)

	err = sshToServer(*server, flagGatewayKey, verbosity)
	if err != nil {
		printError("If you're having issues connecting to your server, you may find some help at https://help.cloud66.com/maestro/how-to-guides/deployment/ssh-to-server.html")
		printFatal(err.Error())
	}
}

func sshToServer(server cloud66.Server, gatewayKey string, verbosity int) error {
	sshFile, err := prepareLocalSshKey(server)
	must(err)

	// open the firewall
	timeToOpen := 20
	fmt.Printf("Opening access to %s...\n", server.Address)
	genericRes, err := client.LeaseSync(server.StackUid, nil, &timeToOpen, nil, &server.Uid)
	must(err)
	if genericRes.Status != true {
		printFatal("Unable to open server lease")
	}

	fmt.Printf("Connecting to %s (%s)...\n", server.Name, server.Address)

	vflag := ""
	if verbosity == 1 {
		vflag = "-v"
	} else if verbosity == 2 {
		vflag = "-vv"
	} else if verbosity == 3 {
		vflag = "-vvv"
	}

	if server.HasDeployGateway {
		tags := []string{
			"ssh",
			"-o", "ProxyCommand='ssh " + server.DeployGatewayUsername + "@" + server.DeployGatewayAddress + " -i  " + gatewayKey + "  nc  " + server.Address + " 22' ",
			"-o", "UserKnownHostsFile=/dev/null",
			"-o", "CheckHostIP=no",
			"-o", "StrictHostKeyChecking=no",
			"-o", "LogLevel=QUIET",
			"-o", "IdentitiesOnly=yes",
			"-A",
			"-p", "22",
			vflag,
			server.UserName + "@" + server.Address,
			"-i", sshFile,
		}
		return startProgram("bash", []string{
			"-c", strings.Join(tags, " "),
		})
	} else {
		return startProgram("ssh", []string{
			server.UserName + "@" + server.Address,
			"-i", sshFile,
			"-o", "UserKnownHostsFile=/dev/null",
			"-o", "CheckHostIP=no",
			"-o", "StrictHostKeyChecking=no",
			"-o", "LogLevel=QUIET",
			"-o", "IdentitiesOnly=yes",
			"-A",
			"-p", "22",
			vflag,
		})
	}
}
