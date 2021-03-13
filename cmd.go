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

var ConnectionTimeout = time.Second * 60
var Retries int = 30
var RetryDelay = time.Second

// Initialize reads flags in from the command line and stands up the atomizer
func Initialize(appname string) error {
	ctx, cancel := context.WithCancel(context.Background())

	// Monitor for sigterm
	sig(ctx, cancel)

	err := initLogger(ctx, appname)
	if err != nil {
		return fmt.Errorf("error setting logger: %s", err.Error())
	}

	// Parse the command line flags or environment variables
	cstring, queue := flags()

	u, err := url.Parse(cstring)
	if err != nil {
		return fmt.Errorf("error parsing connection string | %s", err.Error())
	}

	err = waitForHost(ctx, u, ConnectionTimeout)
	if err != nil {
		return err
	}

	conductor, err := connect(
		ctx,
		cstring,
		queue,
		Retries,
		RetryDelay,
	)
	if err != nil {
		return fmt.Errorf("error creating connection to AMQP | %s", err.Error())
	}

	// Register the conductor into the atomizer library after initializing the
	// connection to the message queue
	err = engine.Register(conductor)
	if err != nil {
		return fmt.Errorf("error registering amqp conductor | %s", err.Error())
	}

	// setup the alog event subscription
	events := make(chan interface{})
	defer close(events)

	alog.Printc(ctx, events)

	// Create a copy of the atomizer
	a, err := engine.Atomize(ctx, events)
	if err != nil {
		return fmt.Errorf("error while initializing atomizer | %s", err.Error())
	}

	// Execute the processing on the atomizer
	err = a.Exec()
	if err != nil {
		return fmt.Errorf("error while executing atomizer | %s", err.Error())
	}

	alog.Println("Online")

	// Block until the processing is interrupted
	a.Wait()

	// Get the alog wait method to work with the internal channels
	alog.Wait(true)

	return nil
}

func connect(
	ctx context.Context,
	cstring,
	queue string,
	retries int,
	retrydelay time.Duration,
) (conductor engine.Conductor, err error) {
	var attempt int
	tick := time.NewTicker(1)
	defer tick.Stop()

	for attempt < retries {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case <-tick.C:
			if attempt == 0 {
				tick.Reset(retrydelay)
			}

			// Create the amqp conductor for the agent
			conductor, err = amqp.Connect(ctx, cstring, queue)

			// Connection established
			if err == nil && conductor != nil {
				alog.Println("conductor connection established")
				return conductor, err
			}

			attempt++
			alog.Printf(
				"connection attempt %d to amqp failed, retrying",
				attempt,
			)
		}
	}

	return nil, fmt.Errorf("error while initializing amqp | %s", err.Error())
}

func initLogger(ctx context.Context, appname string) error {
	// Default empty appname
	if appname == "" {
		appname = "Atomizer Agent"
	}

	return alog.Global(
		ctx,
		appname,
		alog.DEFAULTTIMEFORMAT,
		time.UTC,
		alog.DEFAULTBUFFER,
		alog.Standards()...,
	)
}

// sig monitors for interrupts and exits
func sig(ctx context.Context, cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// Setup interrupt monitoring for the agent
	go func() {
		defer cancel()

		select {
		case <-ctx.Done():
			return
		case <-sigs:
			os.Exit(1)
		}
	}()
}

var ErrTimeout = errors.New("connection timeout exceeded")

// waitForHost waits for the port of the conductor to become available
func waitForHost(ctx context.Context, host *url.URL, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var attempt int
	tick := time.NewTicker(1)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return ErrTimeout
		case <-tick.C:
			if attempt == 0 {
				tick.Reset(time.Second)
			}

			conn, err := net.Dial("tcp", host.Host)
			if err == nil {
				defer conn.Close()

				alog.Printf("host [%s] detected", host)
				return nil
			}

			attempt++
			alog.Printf("waiting for host [%s], attempt %d failed", host.Host, attempt)
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
