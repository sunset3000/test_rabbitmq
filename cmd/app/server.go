package app

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/klog"
)

// serverRun defines the interface to run a specified test server
type serverRun interface {
	Run() error
	CleanupAndExit() error
}

// Options contains everything necessary to create and run a proxy server.
type Options struct {
	// errCh is the channel that errors will be sent
	errCh chan error

	// url of rabbitmq cluster
	mqURL string

	queueCount int

	prodNum int

	consumerCount int

	intervalTime int

	// TestClient is the interface to run the proxy server
	testClient serverRun
}

func NewOptions() *Options {
	return &Options{
		mqURL:         "amqp://admin:123456@10.130.121.131:5672",
		queueCount:    100,
		prodNum:       10000,
		consumerCount: 1000,
		intervalTime:  1000, // millisecond
		errCh:         make(chan error),
	}
}

// AddFlags adds flags to fs and binds them to options.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.mqURL, "mqURL", "amqp://admin:123456@10.130.121.131:5672", "url of rabbitmq cluster")
	fs.IntVar(&o.queueCount, "queueCount", 100, "")
	fs.IntVar(&o.consumerCount, "consumerCount", 1000, "")
	fs.IntVar(&o.prodNum, "prodNum", 10000, "")
	fs.IntVar(&o.intervalTime, "intervalTime", 500, "")
}

// PrintFlags logs the flags in the flagset
func PrintFlags(flags *pflag.FlagSet) {
	flags.VisitAll(func(flag *pflag.Flag) {
		klog.V(1).Infof("FLAG: --%s=%q", flag.Name, flag.Value)
	})
}

func (o *Options) Run() error {
	defer close(o.errCh)

	client, err := NewTestClient(o)
	if err != nil {
		return err
	}

	/*if o.CleanupAndExit {
		return proxyServer.CleanupAndExit()
	}*/

	o.testClient = client
	return o.runLoop()
}

// runLoop will watch on the update change of the proxy server's configuration file.
// Return an error when updated
func (o *Options) runLoop() error {
	// run the test client in goroutine
	go func() {
		err := o.testClient.Run()
		o.errCh <- err
	}()

	for {
		select {
		case err := <-o.errCh:
			if err != nil {
				return err
			}
		}
	}
}

func NewProxyCommand() *cobra.Command {
	opts := NewOptions()

	cmd := &cobra.Command{
		Use: "rabbitmq-test",
		Long: `The Kubernetes network proxy runs on each node. This
reflects services as defined in the Kubernetes API on each node and can do simple
TCP, UDP, and SCTP stream forwarding or round robin TCP, UDP, and SCTP forwarding across a set of backends.
Service cluster IPs and ports are currently found through Docker-links-compatible
environment variables specifying ports opened by the service proxy. There is an optional
addon that provides cluster DNS for these cluster IPs. The user must create a service
with the apiserver API to configure the proxy.`,
		Run: func(cmd *cobra.Command, args []string) {
			//verflag.PrintAndExitIfRequested()
			PrintFlags(cmd.Flags())

			/*if err := opts.Complete(); err != nil {
				klog.Fatalf("failed complete: %v", err)
			}
			if err := opts.Validate(args); err != nil {
				klog.Fatalf("failed validate: %v", err)
			}*/
			klog.Fatal(opts.Run())
		},
	}

	//var err error
	//opts.config, err = opts.ApplyDefaults(opts.config)
	//if err != nil {
	//	klog.Fatalf("unable to create flag defaults: %v", err)
	//}

	opts.AddFlags(cmd.Flags())

	//cmd.MarkFlagFilename("config", "yaml", "yml", "json")

	return cmd
}
