# Kafka Middleman

## Prerequisites

- install docker compose
- install golang.

## Running the Tests

- tried making a test so easy to run and put data
- instead of an application with docker or something.
- also should be more of a library.
- just run test in basic_test.go.

## Example Main Function

```go
package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aryehlev/kafka-middleman/processor"
)

func main() {
	// Configure the processor
	pro := processor.Validator{
		SupportedTimeFormats: []string{time.RFC3339},
	}

	// Create a new Kafka middleware instance
	mid, err := New(
		os.Getenv("GROUP"),
		os.Getenv("IN_TOPIC"),
		os.Getenv("OUT_TOPIC"),
		strings.Split(os.Getenv("BROKERS"), ","),
		pro.Process, // Change processing function as needed. Use NewFromConfig to change serializers and other settings.
	)
	if err != nil {
		log.Fatalf("Failed to create middleware: %v", err)
	}

	// Run the middleware
	if err := mid.Run(context.Background()); err != nil {
		log.Fatalf("Middleware run error: %v", err)
	}
}
```

## Custom Processing Function

You can customize the processing logic by providing a different processing function to the `New` function. The example
above uses a time + length validator, but you can implement any logic you need.

For more advanced configurations, use `NewFromConfig` to adjust serializers and other settings as needed.

## future designs:
- Out of order commmits(handled) with async consumer+processing workers.
- Stay synchronous but smaller concurrent processing buffers.



