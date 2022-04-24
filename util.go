package main

import "strings"

func titelize(input map[string]string) map[string]string {
	output := make(map[string]string)
	for k, v := range input {
		output[strings.Title(k)] = v
	}
	return output
}
