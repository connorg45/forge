package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	apiURL := getenv("FORGE_API_URL", "http://localhost:8080")
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "submit":
		submit(apiURL, os.Args[2:])
	case "ls":
		ls(apiURL, os.Args[2:])
	case "dlq":
		dlq(apiURL, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func submit(apiURL string, args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	queueName := fs.String("queue", "default", "queue name")
	handler := fs.String("handler", "echo", "handler name")
	payload := fs.String("payload", "{}", "JSON payload")
	_ = fs.Parse(args)
	body := map[string]json.RawMessage{
		"payload": json.RawMessage(*payload),
	}
	queueRaw, _ := json.Marshal(*queueName)
	handlerRaw, _ := json.Marshal(*handler)
	body["queue"] = queueRaw
	body["handler"] = handlerRaw
	do("POST", apiURL+"/v1/jobs", body)
}

func ls(apiURL string, args []string) {
	_ = args
	do("GET", apiURL+"/v1/jobs?limit=50", nil)
}

func dlq(apiURL string, args []string) {
	if len(args) >= 2 && args[0] == "requeue" {
		do("POST", apiURL+"/v1/dlq/"+args[1]+"/requeue", nil)
		return
	}
	do("GET", apiURL+"/v1/dlq", nil)
}

func do(method, url string, body any) {
	var reader io.Reader
	if body != nil {
		encoded, _ := json.Marshal(body)
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		panic(err)
	}
	req.Header.Set("content-type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	fmt.Println(string(out))
	if resp.StatusCode >= 300 {
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: forge submit --queue default --handler echo --payload '{\"msg\":\"hi\"}' | forge ls | forge dlq [requeue <id>]")
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
