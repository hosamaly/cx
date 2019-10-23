package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/cloud66/cli"
)

var cmdConfig = &Command{
	Name:       "config",
	Build:      buildConfig,
	NeedsStack: false,
	NeedsOrg:   false,
	Short:      "configuration commands",
}

type configWrapper struct {
	Response *cxConfig `json:"response"`
}

type cxConfig struct {
	APIURI       string `json:"api_url"`
	BaseURI      string `json:"base_url"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	FayeEndpoint string `json:"faye_endpoint"`
}

func buildConfig() cli.Command {
	base := buildBasicCommand()
	base.Subcommands = []cli.Command{
		cli.Command{
			Name:   "list",
			Usage:  "shows the list of all configurations",
			Action: runConfigList,
		},
		cli.Command{
			Name:   "show",
			Usage:  "shows a single configuration values",
			Action: runShowConfig,
		},
		cli.Command{
			Name:   "use",
			Usage:  "switches the configuration to use the given profile",
			Action: runUseConfig,
		},
		cli.Command{
			Name:   "delete",
			Usage:  "delete a given configuration profile",
			Action: runDeleteConfig,
		},
		cli.Command{
			Name:      "create",
			Usage:     "create a new profile",
			ShortName: "create profile_name",
			Action:    runCreateConfig,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "org",
					Usage: "Name of an organization you are a member of. Can be left empty. You can overwrite this using --org argument on other commands",
				},
				cli.StringFlag{
					Name:  "api-url",
					Usage: "URL for Cloud 66 API. Used for OnPrem and Dedicated installations of Cloud 66 Enterprise",
				},
				cli.StringFlag{
					Name:  "base-url",
					Usage: "URL for Cloud 66 Application. Used for OnPrem and Dedicated installations of Cloud 66 Enterprise",
				},
				cli.StringFlag{
					Name:  "client-id",
					Usage: "OAuth Client ID. Used for OnPrem and Dedicated installations of Cloud 66 Enterprise",
				},
				cli.StringFlag{
					Name:  "client-secret",
					Usage: "OAuth Client Secret. Used for OnPrem and Dedicated installations of Cloud 66 Enterprise",
				},
				cli.StringFlag{
					Name:  "faye-endpoint",
					Usage: "URL for realtime push service. Used for OnPrem and Dedicated installations of Cloud 66 Enterprise",
				},
				cli.BoolFlag{
					Name:  "auto",
					Usage: "Tries to pull configuration from the server provided by base-url",
				},
			},
			Description: `
Example:
cx config create foo --org acme
`,
		},
		cli.Command{
			Name:   "rename",
			Usage:  "renames a profile",
			Action: runRenameProfile,
		},
		cli.Command{
			Name:      "update",
			Usage:     "update a profile",
			ShortName: "update profile_name",
			Action:    runUpdateConfig,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "org",
					Usage: "Name of an organization you are a member of. Can be left empty. You can overwrite this using --org argument on other commands",
				},
				cli.StringFlag{
					Name:  "api-url",
					Usage: "URL for Cloud 66 API. Used for OnPrem and Dedicated installations of Cloud 66 Enterprise",
				},
				cli.StringFlag{
					Name:  "base-url",
					Usage: "URL for Cloud 66 Application. Used for OnPrem and Dedicated installations of Cloud 66 Enterprise",
				},
				cli.StringFlag{
					Name:  "client-id",
					Usage: "OAuth Client ID. Used for OnPrem and Dedicated installations of Cloud 66 Enterprise",
				},
				cli.StringFlag{
					Name:  "client-secret",
					Usage: "OAuth Client Secret. Used for OnPrem and Dedicated installations of Cloud 66 Enterprise",
				},
				cli.StringFlag{
					Name:  "faye-endpoint",
					Usage: "URL for realtime push service. Used for OnPrem and Dedicated installations of Cloud 66 Enterprise",
				},
			},
			Description: `
Example:
cx config update foo --org acme
`,
		},
	}

	return base
}

func runConfigList(c *cli.Context) {
	profiles := readProfiles()

	for _, profile := range profiles.Profiles {
		if profile.Name == profiles.LastProfile {
			fmt.Print("* ")
		}

		fmt.Println(profile.Name)
	}
}

func runShowConfig(c *cli.Context) {
	profiles := readProfiles()
	toShow := c.Args().First()
	if toShow == "" {
		printFatal("no profile passed")
	}

	for _, profile := range profiles.Profiles {
		if profile.Name == toShow {
			fmt.Printf("Name: %s\n", profile.Name)
			fmt.Printf("Organization: %s\n", profile.Organization)
			fmt.Println()

			fmt.Printf("ApiURL: %s\n", profile.ApiURL)
			fmt.Printf("BaseURL: %s\n", profile.BaseURL)
			fmt.Printf("FayeEndpoint: %s\n", profile.FayeEndpoint)
			return
		}
	}
}

func runUseConfig(c *cli.Context) {
	toUse := c.Args().First()
	if toUse == "" {
		printFatal("no profile passed")
	}

	profiles := readProfiles()
	if profiles.LastProfile == toUse {
		fmt.Printf("current profile is %s already\n", toUse)
		return
	}

	for _, profile := range profiles.Profiles {
		if profile.Name == toUse {
			profiles.LastProfile = toUse
			if err := profiles.WriteProfiles(); err != nil {
				printFatal("error saving profiles %s", err)
			}

			fmt.Println("profile switched")
			return
		}
	}

	printFatal("no profile found with the given name")
}

func runDeleteConfig(c *cli.Context) {
	toDelete := c.Args().First()
	if toDelete == "" {
		printFatal("no profile passed")
	}

	if toDelete == "default" {
		printFatal("you cannot delete default profile")
	}

	profiles := readProfiles()
	findProfile(profiles, toDelete)

	delete(profiles.Profiles, toDelete)

	if len(profiles.Profiles) == 0 {
		if err := os.Remove(profilePath); err != nil {
			printFatal("error deleting profile %s", err)
		}
	} else {
		if err := profiles.WriteProfiles(); err != nil {
			printFatal("error deleting profile %s", err)
		}
	}

	// do we have a token file? (it might not be there if profile was created by never activated)
	tokenFile := filepath.Join(cxHome(), fmt.Sprintf("cx_%s.json", strings.ToLower(toDelete)))
	if err := os.Remove(tokenFile); err != nil {
		if !os.IsNotExist(err) {
			printFatal("error during removing the token file %s", err)
		}
	}

	fmt.Println("profile deleted")
}

func runCreateConfig(c *cli.Context) {
	defProfile := defaultProfile()

	name := c.Args().First()

	if name == "" {
		printFatal("no name given")
	}

	if name == "default" {
		printFatal("you cannot create a profile called default")
	}

	org := c.String("org")
	apiURL := c.String("api-url")
	baseURL := c.String("base-url")
	fayeEndpoint := c.String("faye-endpoint")
	clientID := c.String("client-id")
	clientSecret := c.String("client-secret")
	auto := c.Bool("auto")

	if apiURL == "" {
		apiURL = defProfile.ApiURL
	}
	if baseURL == "" {
		baseURL = defProfile.BaseURL
	}
	if fayeEndpoint == "" {
		fayeEndpoint = defProfile.FayeEndpoint
	}
	if clientID == "" {
		clientID = defProfile.ClientID
	}
	if clientSecret == "" {
		clientSecret = defProfile.ClientSecret
	}

	if auto {
		config, err := getCxConfig(baseURL)
		if err != nil {
			printFatal(err.Error())
		}

		apiURL = config.APIURI
		fayeEndpoint = config.FayeEndpoint
		clientID = config.ClientID
		clientSecret = config.ClientSecret
	}

	profile := &Profile{
		ApiURL:       apiURL,
		BaseURL:      baseURL,
		FayeEndpoint: fayeEndpoint,
		Organization: org,
		Name:         name,
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenFile:    fmt.Sprintf("cx_%s.json", strings.ToLower(name)),
	}

	profiles := readProfiles()

	for _, p := range profiles.Profiles {
		if p.Name == name {
			printFatal("profile name already exists. Use update or delete instead")
		}
	}

	profiles.Profiles[name] = profile
	if err := profiles.WriteProfiles(); err != nil {
		printFatal("error during saving profiles %s", err)
	}

	fmt.Println("profile created")
}

func runRenameProfile(c *cli.Context) {
	oldName := c.Args().First()

	if oldName == "" {
		printFatal("no old name given")
	}

	if oldName == "default" {
		printFatal("you cannot rename the default profile")
	}

	newName := c.Args().Get(1)
	if newName == "" {
		printFatal("no new name given")
	}

	fmt.Printf("renaming profile %s to %s\n", oldName, newName)

	profiles := readProfiles()
	findProfile(profiles, oldName)

	for _, profile := range profiles.Profiles {
		if profile.Name == newName {
			printFatal("another profile with the same name exists")
		}
	}

	profile := profiles.Profiles[oldName]
	profiles.Profiles[newName] = profile

	delete(profiles.Profiles, oldName)
	profile.Name = newName
	profile.TokenFile = fmt.Sprintf("cx_%s.json", strings.ToLower(newName))

	if err := profiles.WriteProfiles(); err != nil {
		printFatal("error while writing profiles %s", err)
	}

	// rename the token file
	oldTokenFile := filepath.Join(cxHome(), fmt.Sprintf("cx_%s.json", strings.ToLower(oldName)))
	newTokenFile := filepath.Join(cxHome(), fmt.Sprintf("cx_%s.json", strings.ToLower(newName)))

	if err := os.Rename(oldTokenFile, newTokenFile); err != nil {
		if !os.IsNotExist(err) {
			printFatal("error during renaming of the token file %s", err)
		}
	}

	fmt.Println("profile renamed")
}

func runUpdateConfig(c *cli.Context) {
	name := c.Args().First()

	if name == "" {
		printFatal("no name given")
	}

	org := c.String("org")
	apiURL := c.String("api-url")
	baseURL := c.String("base-url")
	fayeEndpoint := c.String("faye-endpoint")
	clientID := c.String("client-id")
	clientSecret := c.String("client-secret")

	profiles := readProfiles()
	profile := findProfile(profiles, name)

	if apiURL == "" {
		apiURL = profile.ApiURL
	}
	if baseURL == "" {
		baseURL = profile.BaseURL
	}
	if fayeEndpoint == "" {
		fayeEndpoint = profile.FayeEndpoint
	}
	if clientID == "" {
		clientID = profile.ClientID
	}
	if clientSecret == "" {
		clientSecret = profile.ClientSecret
	}

	newProfile := &Profile{
		ApiURL:       apiURL,
		BaseURL:      baseURL,
		FayeEndpoint: fayeEndpoint,
		Organization: org,
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Name:         name,
		TokenFile:    fmt.Sprintf("cx_%s.json", strings.ToLower(name)),
	}

	profiles.Profiles[name] = newProfile
	if err := profiles.WriteProfiles(); err != nil {
		printFatal("error during saving profiles %s", err)
	}

	fmt.Println("profile updated")
}

func readProfiles() *Profiles {
	profiles, err := ReadProfiles(profilePath)
	if err != nil {
		printFatal("reading profiles %s", err)
	}

	return profiles
}

func findProfile(profiles *Profiles, name string) *Profile {
	for idx, p := range profiles.Profiles {
		if p.Name == name {
			return profiles.Profiles[idx]
		}
	}

	printFatal("cannot find profile named %s", name)

	return nil
}

func getCxConfig(entryPoint string) (*cxConfig, error) {
	resp, err := http.Get(fmt.Sprintf("%s/api/tooling/cx/config", entryPoint))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, errors.New(resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var wrapper configWrapper
	err = json.Unmarshal(body, &wrapper)
	if err != nil {
		return nil, err
	}

	return wrapper.Response, nil
}
