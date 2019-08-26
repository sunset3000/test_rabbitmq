package main

import (
	"flag"
	"fmt"
	"huayun.com/test_rabbitmq/cmd/app"
	"k8s.io/klog"
	"math/rand"
	"os"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	command := app.NewProxyCommand()

	// TODO: once we switch everything over to Cobra commands, we can go back to calling
	// utilflag.InitFlags() (by removing its pflag.Parse() call). For now, we have to set the
	// normalize func and add the go flag set by hand.
	//utilflag.InitFlags()
	klog.InitFlags(flag.CommandLine)
	flag.Set("logtostderr", "true")

	defer klog.Flush()

	if err := command.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
