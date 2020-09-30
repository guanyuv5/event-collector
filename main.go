package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	broker     string
	kubeconfig string
	topic      string
)

func init() {
	flag.StringVar(&broker, "broker", "127.0.0.1:9200", "kafka brokers.")
	flag.StringVar(&topic, "topic", "k8s-event", "kafka k8s-event topic.")
	flag.StringVar(&kubeconfig, "kubeconfig", "/root/.kube/config", "k8s kubeconfig file path")
	flag.Parse()
	fmt.Println("kafka brokers: ", broker)
}

func loadConfig() kubernetes.Interface {
	fmt.Println("kube config : ", kubeconfig)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset from kubeconfig
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	return clientset
}

// setup a signal hander to gracefully exit
func sigHandler() <-chan struct{} {
	stop := make(chan struct{})
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c,
			syscall.SIGINT,  // Ctrl+C
			syscall.SIGTERM, // Termination Request
			syscall.SIGSEGV, // FullDerp
			syscall.SIGABRT, // Abnormal termination
			syscall.SIGILL,  // illegal instruction
			syscall.SIGFPE)  // floating point - this is why we can't have nice things
		sig := <-c
		glog.Warningf("Signal (%v) Detected, Shutting Down", sig)
		close(stop)
	}()
	return stop
}

func main() {
	var (
		err error
		wg  sync.WaitGroup
	)

	clientset := loadConfig()
	sharedInformers := informers.NewSharedInformerFactory(clientset, time.Second*5)
	eventsInformer := sharedInformers.Core().V1().Events()
	stop := sigHandler()

	// TODO: Support locking for HA https://github.com/kubernetes/kubernetes/pull/42666
	eventRouter := NewEventRouter(clientset, eventsInformer)
	eventRouter.eKafka, err = NewKafkaClient(broker, topic)
	if err != nil {
		panic(err.Error())
	}

	// Startup the EventRouter
	wg.Add(1)
	go func() {
		defer wg.Done()
		eventRouter.Run(stop)
	}()

	// Startup the Informer(s)
	glog.Infof("Starting shared Informer(s)")
	sharedInformers.Start(stop)
	wg.Wait()
	glog.Warningf("Exiting main()")
	os.Exit(1)
}
