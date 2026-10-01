// Command benchgate fails when a `benchstat -format csv` comparison reports a
// performance regression larger than a threshold. See internal/benchgate for
// the policy and the input format.
package main

import (
	"os"

	"github.com/lorenzo-vecchio/gorch/internal/benchgate"
)

func main() {
	os.Exit(benchgate.Run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
