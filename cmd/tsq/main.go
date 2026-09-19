package main

import (
	"fmt"
	"io"
	"os"

	"github.com/tmoeish/tsq/v5/internal/buildinfo"
	"github.com/tmoeish/tsq/v5/internal/cmd"
)

var commands = []*cmd.Command{cmd.GenCmd, cmd.VersionCmd}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, formatCLIError(os.Stderr, err))

		os.Exit(1)
	}
}

// run dispatches args, the command line after the program name.
func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		return usage(out)
	}

	switch args[0] {
	case "-h", "--help":
		return usage(out)
	case "--version":
		_, err := fmt.Fprintf(out, "tsq version %s\n", buildinfo.Version())

		return err
	case "help":
		if len(args) == 1 {
			return usage(out)
		}

		c, err := lookup(args[1])
		if err != nil {
			return err
		}

		c.SetOut(out)

		return c.Help()
	}

	c, err := lookup(args[0])
	if err != nil {
		return err
	}

	c.SetArgs(args[1:])

	return c.Execute()
}

func lookup(name string) (*cmd.Command, error) {
	for _, c := range commands {
		if c.Name == name {
			return c, nil
		}
	}

	return nil, fmt.Errorf("unknown command %q (see tsq --help)", name)
}

func usage(out io.Writer) error {
	_, err := fmt.Fprint(out, "tsq generates type-safe query code for Go structs marked with //tsq: directives.\n\nUsage:\n  tsq <command> [flags]\n\nCommands:\n")
	if err != nil {
		return err
	}

	for _, c := range commands {
		if _, err := fmt.Fprintf(out, "  %-9s %s\n", c.Name, c.Short); err != nil {
			return err
		}
	}

	_, err = fmt.Fprint(out, "\nRun \"tsq help <command>\" for a command's flags; \"tsq --version\" prints the version.\n")

	return err
}

func formatCLIError(file *os.File, err error) string {
	label := "Error"

	if info, statErr := file.Stat(); statErr == nil && info.Mode()&os.ModeCharDevice != 0 && os.Getenv("NO_COLOR") == "" {
		label = "\033[1;31mError\033[0m"
	}

	return fmt.Sprintf("%s: %s", label, err.Error())
}
