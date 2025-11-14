package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sort"
	"time"
)

type StationStats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func parseTemp(tempBytes []byte) float64 {
	negative := false
	index := 0

	if tempBytes[index] == '-' {
		index++
		negative = true
	}

	temp := float64(tempBytes[index] - '0')
	index++

	if tempBytes[index] != '.' {
		temp = temp*10 + float64(tempBytes[index]-'0')
		index++
	}

	index++

	temp += float64(tempBytes[index]-'0') / 10

	if negative {
		return -temp
	}
	return temp
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if flag.NArg() < 1 {
		log.Fatal("Usage: go run main.go [-cpuprofile=cpu.prof] <input_file_path>")
	}
	inputFilePath := flag.Arg(0)

	startTime := time.Now()

	statsMap := make(map[string]*StationStats)

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("Error opening file %s: %v", inputFilePath, err)
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)

	for scanner.Scan() {
		line := scanner.Bytes()

		semicolonIndex := bytes.IndexByte(line, ';')
		if semicolonIndex == -1 {
			continue
		}

		stationNameBytes := line[:semicolonIndex]
		tempBytes := line[semicolonIndex+1:]

		temp := parseTemp(tempBytes)

		stationName := string(stationNameBytes)

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

	if err := writer.Flush(); err != nil {
		log.Fatalf("Error flushing writer: %v", err)
	}

	duration := time.Since(startTime)
	fmt.Printf("Successfully wrote results to output.txt in %s (processed %s)\n", duration, inputFilePath)
}
