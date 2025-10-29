package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type StationStats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run 1brc.go <input_file_path>")
	}
	inputFilePath := os.Args[1]

	startTime := time.Now()

	statsMap := make(map[string]*StationStats)

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("Error opening file %s: %v", inputFilePath, err)
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)

	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.SplitN(line, ";", 2)
		if len(parts) != 2 {
			continue
		}

		stationName := parts[0]
		tempStr := parts[1]

		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			continue
		}

		stats, exists := statsMap[stationName]

		if !exists {
			statsMap[stationName] = &StationStats{
				Min:   temp,
				Max:   temp,
				Sum:   temp,
				Count: 1,
			}
		} else {
			stats.Sum += temp
			stats.Count++
			if temp < stats.Min {
				stats.Min = temp
			}
			if temp > stats.Max {
				stats.Max = temp
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	stationNames := make([]string, 0, len(statsMap))
	for name := range statsMap {
		stationNames = append(stationNames, name)
	}
	sort.Strings(stationNames)

	outputFile, err := os.Create("output.txt")
	if err != nil {
		log.Fatalf("Error creating output file: %v", err)
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	fmt.Fprint(writer, "{")
	for i, stationName := range stationNames {
		stats := statsMap[stationName]
		mean := stats.Sum / float64(stats.Count)

		fmt.Fprintf(writer, "%s=%.1f/%.1f/%.1f", stationName, stats.Min, mean, stats.Max)

		if i < len(stationNames)-1 {
			fmt.Fprint(writer, ", ")
		}
	}
	fmt.Fprintln(writer, "}")

	duration := time.Since(startTime)

	fmt.Printf("Successfully wrote results to output.txt in %s (processed %s)\n", duration, inputFilePath)
}
