package main

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/cloud66-oss/cloud66"
	"github.com/cloud66-oss/cx/term"
	"github.com/mgutz/ansi"
	"gopkg.in/go-yaml/yaml.v2"
)

var lastCommandExecuted *exec.Cmd

// dotYaml represents the .cx.yml file
type dotYamlData struct {
	Args map[string]string `yaml:"args,omitempty"`
}

func (b *dotYamlData) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return unmarshal(&b.Args)
}

func readDotYamlFile(path string) (*dotYamlData, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var t *dotYamlData
	err = yaml.Unmarshal(data, &t)
	if err != nil {
		return nil, err
	}

	return t, err
}

func cxHome() string {
	return filepath.Join(homePath(), ".cloud66")
}

// exists returns whether the given file or directory exists or not
func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func expandPath(filePath string) string {
	if filePath[:2] == "~/" {
		usr, _ := user.Current()
		dir := usr.HomeDir
		return strings.Replace(filePath, "~", dir, 1)
	} else {
		return filePath
	}
}

// concatenates the file content and returns a new one
func appendFiles(files []string, filename string) error {
	if debugMode {
		log.Printf("Concatenating to %s\n", filename)
	}
	d, err := os.Create(filename)
	defer func() {
		cerr := d.Close()
		if err == nil {
			err = cerr
		}
	}()
	if err != nil {
		return err
	}

	for _, fn := range files {
		f, err := os.Open(fn)
		defer f.Close()
		if debugMode {
			log.Printf("Adding %s to %s\n", fn, filename)
		}
		if _, err = io.Copy(d, f); err != nil {
			return err
		}
		err = d.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}

func must(err error) {
	if err != nil {
		if debugMode {
			log.Fatal(err)
		}
		printFatal(err.Error())
	}
}

func printError(message string, args ...interface{}) {
	log.Println(colorizeMessage("red", "error:", message, args...))
}

func printFatal(message string, args ...interface{}) {
	log.Fatal(colorizeMessage("red", "error:", message, args...))
}

func printWarning(message string, args ...interface{}) {
	log.Println(colorizeMessage("yellow", "warning:", message, args...))
}

// potentially needs refactor --> genericresponse type?
func printGenericResponse(genericRes cloud66.GenericResponse) {
	printGenericResponseCustom(genericRes, "", "")
}

func printGenericResponseCustom(genericRes cloud66.GenericResponse, successMessage, failMessage string) {
	var result string
	if genericRes.Status == true {
		result = "Success"
		if genericRes.Message != "" {
			result = result + ": " + genericRes.Message
		} else {
			result = result + "!"
		}
		if successMessage != "" {
			result = fmt.Sprintf("%s\n%s", result, successMessage)
		}
		log.Println(result)
	} else {
		result = "Failed"
		if genericRes.Message != "" {
			result = result + ": " + genericRes.Message
		} else {
			result = result + "!"
		}
		if failMessage != "" {
			result = fmt.Sprintf("%s\n%s", result, failMessage)
		}
		printFatal(result)
	}
}

func mustConfirm(warning, desired string) {
	if term.IsTerminal(os.Stdin) {
		printWarning(warning)
		fmt.Printf("> ")
	}
	var confirm string
	if _, err := fmt.Scanln(&confirm); err != nil {
		printFatal(err.Error())
	}

	if confirm != desired {
		printFatal("Confirmation did not match %q.", desired)
	}
}

func ask(question, passAnswer string) bool {
	fmt.Print(question)
	var confirm string
	if _, err := fmt.Scanln(&confirm); err != nil {
		return false
	}

	return confirm == passAnswer
}

func colorizeMessage(color, prefix, message string, args ...interface{}) string {
	prefResult := ""
	if prefix != "" {
		prefResult = ansi.Color(prefix, color+"+b") + " " + ansi.ColorCode("reset")
	}
	return prefResult + ansi.Color(fmt.Sprintf(message, args...), color) + ansi.ColorCode("reset")
}

func listRec(w io.Writer, a ...interface{}) {
	for i, x := range a {
		fmt.Fprint(w, x)
		if i+1 < len(a) {
			w.Write([]byte{'\t'})
		} else {
			w.Write([]byte{'\n'})
		}
	}
}

type prettyTime struct {
	time.Time
}

func (s prettyTime) String() string {
	sYear := s.Time.Year()
	nYear := time.Now().Year()
	sameYear := sYear == nYear
	if time.Now().Sub(s.Time) < 12*30*24*time.Hour {
		if sameYear {
			return s.Local().Format("Jan _2 15:04")
		} else {
			return s.Local().Format("Jan _2 15:04 2006")
		}
	}
	return s.Local().Format("Jan _2 2006")
}

func HealthText(c cloud66.Container) string {
	switch c.HealthState {
	case cloud66.CNT_HEALTH_UNVERIFIED:
		return "Unverified"
	case cloud66.CNT_HEALTH_UP:
		return fmt.Sprintf("Up (reported by %s)", c.HealthSource)
	case cloud66.CNT_HEALTH_DOWN:
		return fmt.Sprintf("Down (reported by %s)", c.HealthSource)
	case cloud66.CNT_HEALTH_NA:
		return fmt.Sprintf("N/A")
	default:
		return "Unknown health state"
	}
}

type prettyDuration struct {
	time.Duration
}

func (a prettyDuration) String() string {
	switch d := a.Duration; {
	case d > 2*24*time.Hour:
		return a.Unit(24*time.Hour, "d")
	case d > 2*time.Hour:
		return a.Unit(time.Hour, "h")
	case d > 2*time.Minute:
		return a.Unit(time.Minute, "m")
	}
	return a.Unit(time.Second, "s")
}

func (a prettyDuration) Unit(u time.Duration, s string) string {
	return fmt.Sprintf("%2d", roundDur(a.Duration, u)) + s
}

func roundDur(d, k time.Duration) int {
	return int((d + k/2 - 1) / k)
}

func abbrev(s string, n int) string {
	if len(s) > n {
		return s[:n-1] + "…"
	}
	return s
}

func ensurePrefix(val, prefix string) string {
	if !strings.HasPrefix(val, prefix) {
		return prefix + val
	}
	return val
}

func ensureSuffix(val, suffix string) string {
	if !strings.HasSuffix(val, suffix) {
		return val + suffix
	}
	return val
}

func openURL(url string) error {
	var command string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		command = "open"
		args = []string{command, url}
	case "windows":
		command = "cmd"
		args = []string{"/c", "start " + strings.Replace(url, "&", "^&", -1)}
	default:
		if _, err := exec.LookPath("xdg-open"); err != nil {
			log.Println("xdg-open is required to open web pages on " + runtime.GOOS)
			os.Exit(2)
		}
		command = "xdg-open"
		args = []string{command, url}
	}
	return runCommand(command, args, os.Environ())
}

func downloadFile(source string, output string) error {
	out, err := os.Create(output)
	defer out.Close()
	if err != nil {
		return err
	}

	resp, err := http.Get(source)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

// runs the command using OS specific commands
func runCommand(command string, args, env []string) error {
	if runtime.GOOS != "windows" {
		p, err := exec.LookPath(command)
		if err != nil {
			log.Printf("Error finding path to %q: %s\n", command, err)
			os.Exit(2)
		}
		command = p
	}

	if debugMode {
		fmt.Printf("Running Command %s with (%s)\n", command, args)
	}
	return sysExec(command, args, env)
}

func startProgram(command string, args []string) error {

	if runtime.GOOS != "windows" {
		p, err := exec.LookPath(command)
		if err != nil {
			log.Printf("Error finding path to %q: %s\n", command, err)
			os.Exit(2)
		}
		command = p
	}

	if debugMode {
		fmt.Printf("Running Command %s with (%s)\n", command, args)
	}

	cmd := exec.Command(command, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	//TODO: ugly to mock startProgram when testing, need better solution
	if underTest {
		lastCommandExecuted = cmd
		return nil
	}

	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

func clearSshKeyCache() error {
	dir := filepath.Join(homePath(), ".ssh")
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	res := []string{}
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "cx_") && strings.HasSuffix(f.Name(), "_pkey") {
			res = append(res, filepath.Join(dir, f.Name()))
		}
	}

	for _, f := range res {
		err := os.Remove(f)
		if err != nil {
			return err
		}
	}

	return nil
}

func prepareLocalSshKey(server cloud66.Server) (string, error) {
	var sshFile string
	if server.PersonalKey {
		oldFile := filepath.Join(homePath(), ".ssh", "cx_"+server.StackUid)
		if b, _ := fileExists(sshFile); !b {
			os.Remove(oldFile)
		}
		sshFile = filepath.Join(homePath(), ".ssh", "cx_"+server.StackUid+"_pkey")
		// remove the old sshkey
	} else {
		sshFile = filepath.Join(homePath(), ".ssh", "cx_"+server.StackUid)
	}

	// do we have the key?
	if b, _ := fileExists(sshFile); !b {
		// get the content and write the file
		fmt.Println("Fetching SSH key...")
		sshKey, err := client.ServerKeyInformation(server.StackUid, server.Uid)
		if err != nil {
			return "", err
		}
		if err = writeSshFile(sshFile, sshKey); err != nil {
			return "", err
		}
	} else {
		if debugMode {
			fmt.Println("Found an existing SSH key for this server")
		}
	}
	return sshFile, nil
}

func writeSshFile(filename string, content string) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return err
	}
	if _, err := file.WriteString(content); err != nil {
		return err
	}
	return nil
}

func writeFile(filename, content string) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	defer file.Close()
	if err != nil {
		return err
	}
	if _, err := file.WriteString(content); err != nil {
		return err
	}
	return nil
}

// finds the item in the list without case sensitivity, and returns the index
// of the item that matches or begins with the given item
// if more than one match is found, it returns an error
func fuzzyFind(s []string, item string, matchFirstIfMany bool) (int, error) {
	var results []int
	for i := range s {
		// look for identical matches first
		if strings.ToLower(s[i]) == strings.ToLower(item) {
			if matchFirstIfMany {
				return i, nil
			} else {
				results = append(results, i)
			}
		}
	}
	if len(results) == 1 {
		return results[0], nil
	}
	if len(results) > 1 {
		return 0, errors.New("More than one match found for " + item + " you might get better results by passing the environment with -e")
	}

	for i := range s {
		if strings.HasPrefix(strings.ToLower(s[i]), strings.ToLower(item)) {
			if matchFirstIfMany {
				return i, nil
			} else {
				results = append(results, i)
			}
		}
	}

	if len(results) == 0 {
		return 0, errors.New("No match found for " + item)
	}
	if len(results) > 1 {
		return 0, errors.New("More than one match found for " + item)
	}

	return results[0], nil
}

func stringsIndex(s []string, item string) int {
	for i := range s {
		if s[i] == item {
			return i
		}
	}
	return -1
}

func findServer(servers []cloud66.Server, serverName string) (*cloud66.Server, error) {
	// what is provided? IP, name or role?
	// is it an IP?
	ip := net.ParseIP(serverName)
	if ip != nil {
		// it is an IP
		for _, server := range servers {
			if server.Address == serverName {
				// found it.
				return &server, nil
			}
		}
	} else {
		var names []string
		var mappedServers []cloud66.Server
		// collect the server names first
		for _, server := range servers {
			// if its an exact server name match then return the server
			if server.Name == serverName {
				return &server, nil
			}
			names = append(names, server.Name)
			mappedServers = append(mappedServers, server)
		}
		// collect the server roles second
		for _, server := range servers {
			for _, role := range server.Roles {
				names = append(names, role)
				mappedServers = append(mappedServers, server)
			}
		}
		idx, err := fuzzyFind(names, serverName, true)
		if err != nil {
			return nil, err
		}

		return &mappedServers[idx], nil
	}

	return nil, nil
}

var camelingRegex = regexp.MustCompile("[0-9A-Za-z]+")

func camelCase(src string, sep string) string {
	byteSrc := []byte(src)
	chunks := camelingRegex.FindAll(byteSrc, -1)
	for idx, val := range chunks {
		if idx > 0 {
			chunks[idx] = bytes.Title(val)
		}
	}
	return string(bytes.Join(chunks, []byte(sep)))
}

func pascalCase(src string, sep string) string {
	byteSrc := []byte(src)
	chunks := camelingRegex.FindAll(byteSrc, -1)
	for idx, val := range chunks {
		chunks[idx] = bytes.Title(val)
	}
	return string(bytes.Join(chunks, []byte(sep)))
}

func Tar(source, target string) error {
	tarfile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer tarfile.Close()

	tarball := tar.NewWriter(tarfile)
	defer tarball.Close()

	info, err := os.Stat(source)
	if err != nil {
		return nil
	}

	var baseDir string
	if info.IsDir() {
		baseDir = filepath.Base(source)
	}

	return filepath.Walk(source,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			header, err := tar.FileInfoHeader(info, info.Name())
			if err != nil {
				return err
			}

			if baseDir != "" {
				header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, source))
			}

			if err := tarball.WriteHeader(header); err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = io.Copy(tarball, file)
			return err
		})
}

func Untar(tarball, target string) error {
	reader, err := os.Open(tarball)
	if err != nil {
		return err
	}
	defer reader.Close()
	tarReader := tar.NewReader(reader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		path := filepath.Join(target, header.Name)
		info := header.FileInfo()
		if info.IsDir() {
			if err = os.MkdirAll(path, info.Mode()); err != nil {
				return err
			}
			continue
		}

		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(file, tarReader)
		if err != nil {
			return err
		}
	}

	return nil
}
