package cmd

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// Command is one tsq subcommand on the standard library's flag package: a help
// text, flags declared afresh for every run, and the function that runs it.
type Command struct {
	// Name is the subcommand, as typed after "tsq".
	Name string
	// Usage is the synopsis after "tsq".
	Usage string
	// Short is the one-line summary the root help lists.
	Short string
	// Long is the help text.
	Long string
	// Example lists example invocations, already indented.
	Example string

	flags func(fs *flag.FlagSet)
	run   func(c *Command, args []string) error

	args   []string
	out    io.Writer
	errOut io.Writer
}

// SetArgs sets the arguments of the next Execute, after the subcommand name.
func (c *Command) SetArgs(args []string) { c.args = args }

// SetOut sets where the command writes its output; standard output by default.
func (c *Command) SetOut(w io.Writer) { c.out = w }

// SetErr sets where the command writes diagnostics; standard error by default.
func (c *Command) SetErr(w io.Writer) { c.errOut = w }

// OutOrStdout returns the output writer.
func (c *Command) OutOrStdout() io.Writer {
	if c.out == nil {
		return os.Stdout
	}

	return c.out
}

// ErrOrStderr returns the diagnostics writer.
func (c *Command) ErrOrStderr() io.Writer {
	if c.errOut == nil {
		return os.Stderr
	}

	return c.errOut
}

// flagSet declares the command's flags on a new set, so no state survives
// between runs.
func (c *Command) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("tsq "+c.Name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	if c.flags != nil {
		c.flags(fs)
	}

	return fs
}

// Execute parses the arguments set with SetArgs and runs the command. Flags may
// come before or after the positional arguments; "-h" and "--help" print the
// help instead.
func (c *Command) Execute() error {
	fs := c.flagSet()

	args, err := parseInterspersed(fs, c.args)
	if errors.Is(err, flag.ErrHelp) {
		return c.Help()
	}

	if err != nil {
		return fmt.Errorf("%w (see tsq %s --help)", err, c.Name)
	}

	return c.run(c, args)
}

// parseInterspersed parses flags wherever they appear among the positional
// arguments, which flag.Parse alone stops at, and returns the positional ones.
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string

	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}

		args = fs.Args()
		if len(args) == 0 {
			return positional, nil
		}

		if args[0] == "--" {
			return append(positional, args[1:]...), nil
		}

		positional = append(positional, args[0])
		args = args[1:]
	}
}

// Help writes the command's help text, synopsis, examples and flags.
func (c *Command) Help() error {
	var b strings.Builder

	b.WriteString(c.Long)
	b.WriteString("\n\nUsage:\n  tsq ")
	b.WriteString(c.Usage)
	b.WriteString("\n")

	if c.Example != "" {
		b.WriteString("\nExamples:\n")
		b.WriteString(c.Example)
		b.WriteString("\n")
	}

	b.WriteString("\nFlags:\n")

	fs := c.flagSet()
	fs.VisitAll(func(f *flag.Flag) {
		fmt.Fprintf(&b, "  %-12s %s\n", "--"+f.Name, f.Usage)
	})
	fmt.Fprintf(&b, "  %-12s %s\n", "-h, --help", "help for "+c.Name)

	_, err := io.WriteString(c.OutOrStdout(), b.String())

	return err
}

// isTerminal reports whether w is a terminal, where colored output belongs. It
// honors NO_COLOR.
func isTerminal(w io.Writer) bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}

	f, ok := w.(*os.File)
	if !ok {
		return false
	}

	info, err := f.Stat()

	return err == nil && info.Mode()&os.ModeCharDevice != 0
}
