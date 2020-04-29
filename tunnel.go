package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/cloud66-oss/cloud66"

	"github.com/cloud66/cli"
)

var cmdTunnel = &Command{
	Name:  "tunnel",
	Build: buildBasicCommand,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "server",
			Usage: "server to tunnel to",
		},
		cli.IntFlag{
			Name:  "local,l",
			Usage: "local port for the tunnel",
		},
		cli.IntFlag{
			Name:  "remote,r",
			Usage: "remote port for the tunnel",
		},
	},
	Run:        runTunnel,
	NeedsStack: true,
	NeedsOrg:   false,
	Short:      "opens an SSH tunnel between the local host and a remote server",
	Long: `This command opens an SSH tunnel between your local machine and the remote server on a specific port.

This is useful when connecting to remote databases or other services using local tools. 
For this purpose, tunnel performs the following:

1. Open a lease in port 22 for your local IP address
2. Fetch your SSH key from your Cloud 66 acccount
3. Start an SSH tunnel beween your machine and the server on the given ports
4. Close the tunnel when you leave cx

To exit, use Ctrl-C

You need to have the correct access permissions to use this command.
You can use either the server name (ie lion) or the server IP (ie. 123.123.123.123) or the server role (ie. web)
with thie command.

This command doesn't work with stacks using gateways (Bastion servers).

If a role is specified the command will connect to the first server with that role.
Names are case insensitive and will work with the starting characters as well.

This command is only supported on Linux and OS X (for Windows you can run this in a virtual machine if necessary)

To specify the ports for the tunnel, use --local and --remote options. 

For example, if you need to connect to a MySQL server, you can use 3307 locally and 3306 (MySQL port on the server) as the remote port.
Once the tunnel is established, you can use your favourite MySQL client to connect to the server on 127.0.0.1 and the local port (3307 in this case).

If a local port is not specified, cx will use remote + 1 as a convention for the local port.
For example, if you only specify --remote 5432 without explicitly specifying local, cx will use 5433 as the local port.

Examples:
$ cx tunnel -s mystack --server lion --local 3307 --remote 3306
$ cx tunnel -s mystack --server 52.65.34.98 --local 3307 --remote 3306
$ cx tunnel -s mystack --server web -l 3307 -r 3306
`,
}

func runTunnel(c *cli.Context) {
	if runtime.GOOS == "windows" {
		printFatal("Not supported on Windows")
		os.Exit(2)
	}

	stack := mustStack(c)
	serverName := c.String("server")
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
	if !c.IsSet("remote") {
		printFatal("No remote port specified. Use --remote")
	}
	remotePort := c.Int("remote")
	localPort := remotePort + 1
	if c.IsSet("local") {
		localPort = c.Int("local")
	}

	err = TunnelToServer(*server, localPort, remotePort)
	if err != nil {
		printFatal(err.Error())
	}
}

func TunnelToServer(server cloud66.Server, localPort int, remotePort int) error {
	sshFile, err := prepareLocalSshKey(server)
	must(err)

	// open the firewall
	var timeToOpen = 2
	genericRes, err := client.LeaseSync(server.StackUid, nil, &timeToOpen, nil, &server.Uid)
	must(err)
	if genericRes.Status != true {
		printFatal("Unable to open server lease")
	}

	fmt.Printf("Opening Tunnel from local:%d to %s:%d (127.0.0.1:%d to %s:%d)...\n", localPort, server.Name, remotePort, localPort, server.Address, remotePort)
	fmt.Println("Press Ctrl-C to exit")

	return startProgram("ssh", []string{
		server.UserName + "@" + server.Address,
		"-i", sshFile,
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "CheckHostIP=no",
		"-o", "StrictHostKeyChecking=no",
		"-o", "LogLevel=QUIET",
		"-o", "IdentitiesOnly=yes",
		"-N",
		"-L", fmt.Sprintf("%d:%s:%d", localPort, server.Address, remotePort),
		"-p", "22",
	})
}
