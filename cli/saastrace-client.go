package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type Check struct {
	Endpoint string `json:"endpoint"`
	Target   string `json:"target"`
}

func New(endpoint, target string) Check {
	var c Check
	c = Check{Endpoint: endpoint, Target: target}
	return c
}

var functionUrls = [...]string{
	"https://us-east1-ls-poc-land.cloudfunctions.net/HTTPCheck",
	"https://us-central1-ls-poc-land.cloudfunctions.net/HTTPCheck",
	"https://europe-west1-ls-poc-land.cloudfunctions.net/HTTPCheck",
	"https://asia-northeast1-ls-poc-land.cloudfunctions.net/HTTPCheck",
}

func worker(id int, jobs <-chan Check, results chan<- string) {
	for j := range jobs {
		for _, url := range functionUrls {
			results <- j.HTTPCheck(url)
			fmt.Printf("%v processed job\n", id)
		}
	}
}

func (c Check) HTTPCheck(url string) string {
	encBody, err := json.Marshal(c)
	if err != nil {
		return fmt.Sprintf("bad structure")
	}
	body := bytes.NewReader(encBody)
	resp, err := http.Post(url, "application/json", body)
	if err != nil {
		return fmt.Sprintf("bad post")
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf("bad read")
	}

	return fmt.Sprintf(string(respBody))
}

func main() {
	jobs := make(chan Check, 100)
	results := make(chan string, 10000)

	for w := 1; w <= 10; w++ {
		go worker(w, jobs, results)
	}

	for s := 1; s <= 1024; s *= 2 {
		for i := 0; i < 10; i++ {
			target := fmt.Sprintf("source/%v_1k.obj", i)
			jobs <- New("GCS", target)
		}
		time.Sleep(time.Duration(s) * time.Second)
	}

	close(jobs)

	for i := 0; i < 600; i++ {
		<-results
	}
}
