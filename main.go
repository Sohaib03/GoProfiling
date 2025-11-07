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
	"strconv"
	"time"
)

type StationStats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

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
		// OPTIMIZATION 1: Use Bytes() instead of Text() to avoid allocation for the whole line.
		// This slice is reused by the scanner on the next call to Scan().
		line := scanner.Bytes()

		// OPTIMIZATION 2: Manually find the semicolon index.
		// Much faster than strings.SplitN and doesn't allocate a slice for the parts.
		semicolonIndex := bytes.IndexByte(line, ';')
		if semicolonIndex == -1 {
			continue
		}

		stationNameBytes := line[:semicolonIndex]
		tempBytes := line[semicolonIndex+1:]

		// OPTIMIZATION 3: Use compiler optimization for map lookup.
		// `statsMap[string(byteSlice)]` does NOT allocate a real string if used just for lookup.
		stats, exists := statsMap[string(stationNameBytes)]

		// NOTE: We still have to convert tempBytes to string for strconv.ParseFloat.
		// Eliminating this requires a custom byte-to-float parser (more advanced).
		temp, err := strconv.ParseFloat(string(tempBytes), 64)
		if err != nil {
			continue
		}

		if !exists {
			// OPTIMIZATION 4: Only allocate the actual string memory if it's a new station.
			statsMap[string(stationNameBytes)] = &StationStats{
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
