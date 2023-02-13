package emulator

import "strings"

// arrayFlags used for parsing list of potentially repeated flags e.g. -queue $Q1 -queue $Q2
type arrayFlags []string

func (i *arrayFlags) String() string {
	return strings.Join(*i, ", ")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)

	return nil
}
