package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66/cli"
)

func buildStacksSSL() cli.Command {
	return cli.Command{
		Name:  "ssl",
		Usage: "commands to work with SSL certificates",
		Subcommands: []cli.Command{
			cli.Command{
				Name:   "add",
				Action: addSSLCertificate,
				Usage:  "add an SSL certificate to a stack",
				Flags: []cli.Flag{
					buildStackFlag(),
					cli.StringFlag{
						Name:  "type",
						Usage: fmt.Sprintf("type of the SSL certificate (one of '%s', or '%s')", cloud66.LetsEncryptSslCertificateType, cloud66.ManualSslCertificateType),
					},
					cli.BoolFlag{
						Name:  "ssl-termination",
						Usage: "enable SSL termination",
					},
					cli.StringFlag{
						Name:  "cert",
						Usage: fmt.Sprintf("SSL certificate file path (required for type '%s')", cloud66.ManualSslCertificateType),
					},
					cli.StringFlag{
						Name:  "key",
						Usage: fmt.Sprintf("SSL key file path (required for type '%s')", cloud66.ManualSslCertificateType),
					},
					cli.StringFlag{
						Name:  "intermediate",
						Usage: fmt.Sprintf("SSL intermediate certificate file path (optional for type '%s')", cloud66.ManualSslCertificateType),
					},
					cli.StringSliceFlag{
						Name:  "domain",
						Usage: fmt.Sprintf("Domain name applicable to this SSL certificate (required for type '%s', optional for type '%s'). Repeatable for multiple domains", cloud66.LetsEncryptSslCertificateType, cloud66.ManualSslCertificateType),
						Value: &cli.StringSlice{},
					},
					cli.BoolFlag{
						Name:  "overwrite",
						Usage: "required confirmation flag to update an existing SSL certificate",
					},
				},
				Description: buildStacksSSLAddDescription(),
			},
		},
	}
}

func buildStacksSSLAddDescription() string {
	return `Add an SSL certificate to a stack.

EXAMPLES:
    $ cx stacks ssl add -s my-stack --type lets_encrypt --domain 'web.domain.com' --domain 'api.domain.com'
    $ cx stacks ssl add -s my-stack --type manual --cert certificate_file_path --key key_file_path --intermediate intermediate_file_path
`
}

func addSSLCertificate(c *cli.Context) {
	stack := mustStack(c)

	sslCertificates, err := client.ListSslCertificates(stack.Uid)
	if err != nil {
		printFatal(err.Error())
	}

	const createSSLCertificate = "create"
	const updateSSLCertificate = "update"

	sslCertificateOperation := ""
	if len(sslCertificates) != 0 {
		if !c.Bool("overwrite") {
			printFatal("SSL certificate already exists for this application. Please use the --overwrite flag if you want to overwrite the existing certificate.")
		}
		sslCertificateOperation = updateSSLCertificate
	} else {
		sslCertificateOperation = createSSLCertificate
	}

	sslCertificate, err := generateSSLCertificate(c)
	if err != nil {
		printFatal(err.Error())
	}

	var successMessage string
	switch sslCertificateOperation {
	case createSSLCertificate:
		sslCertificate, err = client.CreateSslCertificate(stack.Uid, sslCertificate)
		successMessage = "Creating SSL certificate..."
	case updateSSLCertificate:
		sslCertificateUUID := sslCertificates[0].Uuid
		sslCertificate, err = client.UpdateSslCertificate(stack.Uid, sslCertificateUUID, sslCertificate)
		successMessage = "Updating SSL certificate..."
	}
	if err != nil {
		printFatal(err.Error())
	}

	fmt.Println(successMessage)
}

func generateSSLCertificate(c *cli.Context) (*cloud66.SslCertificate, error) {
	switch c.String("type") {
	case cloud66.LetsEncryptSslCertificateType:
		return generateLetsEncryptSSLCertificate(c)
	case cloud66.ManualSslCertificateType:
		return generateManualSSLCertificate(c)
	default:
		errorMessage := fmt.Sprintf("Please ensure that you specify the SSL certificate type with the --type flag (one of '%s', or '%s').", cloud66.LetsEncryptSslCertificateType, cloud66.ManualSslCertificateType)
		return nil, errors.New(errorMessage)
	}
}

func generateLetsEncryptSSLCertificate(c *cli.Context) (*cloud66.SslCertificate, error) {
	invalidFlags := []string{"cert", "key", "intermediate"}
	for _, invalidFlag := range invalidFlags {
		if c.String(invalidFlag) != "" {
			return nil, errors.New(fmt.Sprintf("The following flags are not valid for Let's Encrypt certificates: %s. Please remove them and try again.", strings.Join(invalidFlags, ", ")))
		}
	}

	domains := c.StringSlice("domain")
	if len(domains) == 0 {
		return nil, errors.New("No domain names specified. Please use the repeatable --domain flag to specify domain names.")
	}

	return &cloud66.SslCertificate{
		Type:           cloud66.LetsEncryptSslCertificateType,
		ServerNames:    generateSSLCertificateServerNames(c),
		SSLTermination: c.Bool("ssl-termination"),
	}, nil
}

func generateManualSSLCertificate(c *cli.Context) (*cloud66.SslCertificate, error) {
	certificateFile := c.String("cert")
	if certificateFile == "" {
		return nil, errors.New("No certificate file specified. Please use the --cert flag to specify it.")
	}
	certificateFileData, err := ioutil.ReadFile(certificateFile)
	if err != nil {
		return nil, err
	}
	certificate := string(certificateFileData)

	keyFile := c.String("key")
	if keyFile == "" {
		return nil, errors.New("No key file specified. Please use the --key flag to specify it.")
	}
	keyFileData, err := ioutil.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	key := string(keyFileData)

	intermediateFile := c.String("intermediate")
	var intermediatePointer *string
	if intermediateFile != "" {
		intermediateFileData, err := ioutil.ReadFile(intermediateFile)
		if err != nil {
			return nil, err
		}
		intermediate := string(intermediateFileData)
		intermediatePointer = &intermediate
	}

	return &cloud66.SslCertificate{
		Type:                    cloud66.ManualSslCertificateType,
		ServerNames:             generateSSLCertificateServerNames(c),
		Certificate:             &certificate,
		Key:                     &key,
		IntermediateCertificate: intermediatePointer,
		SSLTermination:          c.Bool("ssl-termination"),
	}, nil
}

func generateSSLCertificateServerNames(c *cli.Context) string {
	return strings.Join(c.StringSlice("domain"), ",")
}
