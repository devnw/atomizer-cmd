// Copyright © 2019 Developer Network, LLC
//
// This file is subject to the terms and conditions defined in
// file 'LICENSE', which is part of this source code package.

package cmd

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"atomizer.io/amqp"
	"atomizer.io/engine"
	"devnw.com/alog"
	"github.com/pkg/errors"
)

const (
	// connectionStringENV is the connection string for the message queue,
	// in this case this is specific to rabbitmq
	connectionStringENV string = "CONNECTIONSTRING"

	// QUEUE is the queue for atom messages to be passed across in the
	// message queue
	queueENV string = "QUEUE"
)

// Initialize reads flags in from the command line and stands up the atomizer
func Initialize(appname string) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())

	// Setup interrupt monitoring for the agent
	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
			return
		case <-sigs:
			alog.Printf("Closing %s", appname)
			os.Exit(1)
		}
	}()

	err := alog.Global(
		ctx,
		appname,
		alog.DEFAULTTIMEFORMAT,
		time.UTC,
		alog.DEFAULTBUFFER,
		alog.Standards()...,
	)

	if err != nil {
		fmt.Println("unable to overwrite the global logger")
	}

	// Parse the command line flags or environment variables
	cstring, queue := flags()

	u, err := url.Parse(cstring)
	if err != nil {
		fmt.Println("error parsing connection string")
		os.Exit(1)
	}

	if err = waitForHost(ctx, u.Host); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var conductor engine.Conductor
	var attempts int

	for attempts < 5 && conductor == nil {

		// Create the amqp conductor for the agent
		conductor, err = amqp.Connect(ctx, cstring, queue)
		if err != nil || conductor == nil {
			if attempts >= 5 {
				fmt.Println(
					"error while initializing amqp | " +
						err.Error(),
				)
				os.Exit(1)
			}

			attempts++
			alog.Printf(
				"connection attempt %d to amqp failed, retrying",
				attempts,
			)
			time.Sleep(time.Second * 5)
		}
	}

	alog.Println("connection to amqp established")

	// Register the conductor into the atomizer library after initializing the
	// connection to the message queue
	err = engine.Register(conductor)
	if err != nil {
		fmt.Println("error registering amqp conductor | " + err.Error())
		os.Exit(1)
	}

	// setup the alog event subscription
	events := make(chan interface{})
	defer close(events)

	alog.Printc(ctx, events)

	// Create a copy of the atomizer
	a, err := engine.Atomize(ctx, events)
	if err != nil {
		fmt.Println("error while initializing atomizer.| " + err.Error())
		os.Exit(1)
	}

	// Execute the processing on the atomizer
	err = a.Exec()
	if err != nil {
		fmt.Println("error while executing atomizer.| " + err.Error())
		os.Exit(1)
	}

	alog.Println("Online")

	// Block until the processing is interrupted
	a.Wait()

	time.Sleep(time.Millisecond * 50)

	// Get the alog wait method to work with the internal channels
	alog.Wait(true)
}

// waitForHost waits for the port of the conductor to become available
func waitForHost(ctx context.Context, host string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return errors.New("connection timeout exceeded")

		default:
			_, err := net.Dial("tcp", host)
			if err == nil {
				alog.Printf("host [%s] detected", host)
				return nil
			}

			alog.Printf("waiting for host [%s]", host)
			time.Sleep(time.Second * 5)
		}
	}

}

func flags() (conductor, queue string) {
	c := flag.String(
		"conn",
		"amqp://guest:guest@localhost:5672/",
		"connection string used for rabbit mq",
	)

	q := flag.String(
		"queue",
		"atomizer",
		"queue is the queue for atom messages to be passed across in the message queue",
	)

	flag.Parse()

	return environment(*c, *q)
}

// environment pulls the environment variables as defined in the constants
// section and overwrites the passed flag values
func environment(cflag, qflag string) (c, q string) {

	c = os.Getenv(connectionStringENV)
	if c == "" {
		c = cflag
	}

	q = os.Getenv(queueENV)
	if q == "" {
		q = qflag
	}

	return c, q
}
