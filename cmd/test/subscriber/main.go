package main


import (
	"flag"
"log"
	"google.golang.org/grpc"
	"github.com/murphybytes/gots/api"

	"os"
	"os/signal"
)

func main() {
	var serverIPAddress string
	var workerCount int
	flag.StringVar(&serverIPAddress, "server", "", "IP address of time series server")
	flag.IntVar(&workerCount, "worker-count", 10, "count of workers making requests")
	flag.Parse()
	closer := make(chan struct{})
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	conn, err := grpc.Dial(serverIPAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect: %s", err)
	}
	defer conn.Close()




	<-sig
	close(closer)



}
