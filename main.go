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
	Min   int64
	Max   int64
	Sum   int64
	Count int64
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

const TableSize = 1 << 17 // ~131,072 slots

type StationItem struct {
	Key   []byte // The station name
	Stats StationStats
}

func hash(name []byte) uint64 {
	var h uint64 = 14695981039346656037
	for _, b := range name {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
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

	var items [TableSize]StationItem

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("Error opening file %s: %v", inputFilePath, err)
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)

	for scanner.Scan() {
		line := scanner.Bytes()

		end := len(line)

		tenths := int64(line[end-1] - '0')

		ones := int64(line[end-3] - '0')

		var temp int64
		var stationNameBytes []byte

		c := line[end-4]

		if c == ';' {
			// Format: N.N
			temp = ones*10 + tenths
			stationNameBytes = line[:end-4] // Semicolon is here
		} else if c == '-' {
			// Format: -N.N
			temp = -(ones*10 + tenths)
			stationNameBytes = line[:end-5] // Semicolon is one back
		} else {
			// Format: NN.N or -NN.N
			tens := int64(c - '0')
			if line[end-5] == ';' {
				// Format: NN.N
				temp = tens*100 + ones*10 + tenths
				stationNameBytes = line[:end-5]
			} else {
				// Format: -NN.N
				temp = -(tens*100 + ones*10 + tenths)
				stationNameBytes = line[:end-6]
			}
		}

		h := hash(stationNameBytes)
		idx := h & (TableSize - 1)

		for {
			if items[idx].Key == nil {
				key := make([]byte, len(stationNameBytes))
				copy(key, stationNameBytes)
				items[idx].Key = key

				items[idx].Stats.Min = temp
				items[idx].Stats.Max = temp
				items[idx].Stats.Sum = temp
				items[idx].Stats.Count = 1
				break
			}

			if bytes.Equal(items[idx].Key, stationNameBytes) {
				s := &items[idx].Stats
				s.Sum += temp
				s.Count++
				if temp < s.Min {
					s.Min = temp
				}
				if temp > s.Max {
					s.Max = temp
				}
				break
			}

			idx = (idx + 1) & (TableSize - 1)
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	var sortedStations []string
	results := make(map[string]*StationStats)

	for i := range items {
		if items[i].Key == nil {
			continue
		}
		name := string(items[i].Key)
		sortedStations = append(sortedStations, name)
		results[name] = &items[i].Stats
	}
	sort.Strings(sortedStations)

	outputFile, err := os.Create("output.txt")
	defer outputFile.Close()
	writer := bufio.NewWriter(outputFile)

	fmt.Fprint(writer, "{")
	for i, name := range sortedStations {
		stats := results[name]
		mean := float64(stats.Sum) / float64(stats.Count) / 10.0
		fmt.Fprintf(writer, "%s=%.1f/%.1f/%.1f", name, float64(stats.Min)/10.0, mean, float64(stats.Max)/10.0)

		if i < len(sortedStations)-1 {
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
