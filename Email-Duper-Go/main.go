package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

const (
	RED    = "\033[31m"
	GREEN  = "\033[32m"
	YELLOW = "\033[33m"
	BLUE   = "\033[34m"
	RESET  = "\033[0m"
)

type Config struct {
	Filename string `json:"filename"`
	NewFile  string `json:"new_file"`
}

func main() {
	// Read config.json
	fmt.Println(BLUE + "Reading config.json..." + RESET)
	configFile, err := os.ReadFile("config.json")
	if err != nil {
		fmt.Println(RED+"Error reading config.json:", err, RESET)
		pause()
		return
	}

	var config Config
	if err := json.Unmarshal(configFile, &config); err != nil {
		fmt.Println(RED+"Error parsing config.json:", err, RESET)
		pause()
		return
	}

	// Open the email file
	fmt.Println(BLUE + "Opening file: " + config.Filename + RESET)
	file, err := os.Open(config.Filename)
	if err != nil {
		fmt.Println(RED+"Error opening file:", err, RESET)
		pause()
		return
	}
	defer file.Close()

	// Read emails and remove duplicates using concurrency
	emailSet := sync.Map{}
	var totalEmails, duplicateCount int
	var wg sync.WaitGroup

	scanner := bufio.NewScanner(file)
	lines := make(chan string, 10000) // Buffered channel for concurrency

	// Worker function to process emails concurrently
	worker := func() {
		for email := range lines {
			email = strings.TrimSpace(email)
			if email != "" {
				if _, exists := emailSet.Load(email); exists {
					duplicateCount++
				} else {
					emailSet.Store(email, struct{}{})
				}
			}
		}
		wg.Done()
	}

	// Launch multiple workers
	for i := 0; i < 8; i++ { // 8 workers
		wg.Add(1)
		go worker()
	}

	// Read file and send lines to workers
	for scanner.Scan() {
		lines <- scanner.Text()
		totalEmails++
		if totalEmails%100000 == 0 {
			fmt.Printf(YELLOW+"Processed: %d emails, Duplicates: %d\r"+RESET, totalEmails, duplicateCount)
		}
	}
	close(lines)
	wg.Wait()

	if err := scanner.Err(); err != nil {
		fmt.Println(RED+"Error reading file:", err, RESET)
		pause()
		return
	}

	// Write unique emails to a new file
	fmt.Println(GREEN + "Writing unique emails to " + config.NewFile + "..." + RESET)
	outputFile, err := os.Create(config.NewFile)
	if err != nil {
		fmt.Println(RED+"Error creating output file:", err, RESET)
		pause()
		return
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	emailSet.Range(func(email, _ interface{}) bool {
		fmt.Fprintln(writer, email)
		return true
	})
	writer.Flush()

	fmt.Println(GREEN + "Duplicate removal complete. Unique emails saved to " + config.NewFile + RESET)
	fmt.Printf(GREEN+"Total Emails Processed: %d, Unique Emails: %d, Duplicates Found: %d\n"+RESET, totalEmails, totalEmails-duplicateCount, duplicateCount)
	pause()
}

func pause() {
	fmt.Println(BLUE + "Press Enter to exit..." + RESET)
	bufio.NewReader(os.Stdin).ReadBytes('\n')
}
