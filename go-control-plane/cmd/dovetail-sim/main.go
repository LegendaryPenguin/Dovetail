// Command dovetail-sim runs the scheduler against synthetic multi-tenant
// workloads and reports the comparisons in bench/results.md.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/LegendaryPenguin/dovetail/simulator"
)

func main() {
	group := flag.String("group", "", "run one group (quota, fairness, packing, failure, gangs, cycle); empty runs all")
	flag.Parse()

	all := simulator.All()
	order := simulator.GroupOrder
	if *group != "" {
		if _, ok := all[*group]; !ok {
			fmt.Fprintf(os.Stderr, "unknown group %q\n", *group)
			os.Exit(2)
		}
		order = []string{*group}
	}

	invalid := 0
	for _, name := range order {
		fmt.Printf("\n########## %s ##########\n", name)
		for _, sc := range all[name] {
			result, err := simulator.Run(sc)
			if err != nil {
				fmt.Fprintf(os.Stderr, "scenario %q failed: %v\n", sc.Name, err)
				os.Exit(1)
			}
			fmt.Print(result.Report())
			invalid += result.AccountingErrors
		}
	}

	if invalid > 0 {
		// An accounting error means the ledger disagreed with the task state,
		// which invalidates every number above it. Exiting non-zero so this
		// cannot be mistaken for a successful run in a script.
		fmt.Fprintf(os.Stderr, "\n%d accounting errors across all scenarios: results are not trustworthy\n", invalid)
		os.Exit(1)
	}
}
