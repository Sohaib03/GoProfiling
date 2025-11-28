package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

type StationStats struct {
	Min   int64
	Max   int64
	Sum   int64
	Count int64
}

const TableSize = 1 << 17

type StationItem struct {
	Key   []byte
	Stats StationStats
}

type ChunkResult struct {
	Items [TableSize]StationItem
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func hash(name []byte) uint64 {
	var h uint64 = 14695981039346656037
	for _, b := range name {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}

func processChunk(filePath string, startOffset, endOffset int64, wg *sync.WaitGroup, resultChan chan<- *ChunkResult) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Worker error opening file: %v", err)
		return
	}
	defer file.Close()

	_, err = file.Seek(startOffset, 0)
	if err != nil {
		log.Printf("Worker error seeking: %v", err)
		return
	}

	buf := make([]byte, 1024*1024)

	var result ChunkResult

	if startOffset != 0 {
		tempBuf := make([]byte, 1)
		for {
			_, err := file.Read(tempBuf)
			if err != nil {
				if err == io.EOF {
					return
				}
				log.Fatal(err)
			}
			startOffset++
			if tempBuf[0] == '\n' {
				break
			}
		}
	}

	leftover := make([]byte, 0, 100)
	currentPos := startOffset

	for {
		n, err := file.Read(buf)
		if n == 0 || err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		chunk := buf[:n]
		processedBytes := 0

		for {
			newlineIdx := bytes.IndexByte(chunk[processedBytes:], '\n')

			if newlineIdx == -1 {
				leftover = append(leftover, chunk[processedBytes:]...)
				break
			}

			actualIdx := processedBytes + newlineIdx

			var line []byte
			if len(leftover) > 0 {
				line = append(leftover, chunk[processedBytes:actualIdx]...)
				leftover = leftover[:0]
			} else {
				line = chunk[processedBytes:actualIdx]
			}

			processLine(line, &result.Items)

			processedBytes = actualIdx + 1
			currentPos += int64(len(line) + 1)
		}

		currentFilePos, _ := file.Seek(0, 1)
		if currentFilePos > endOffset && len(leftover) == 0 {
			break
		}
	}

	resultChan <- &result
}

func processLine(line []byte, items *[TableSize]StationItem) {
	end := len(line)
	if end < 4 {
		return
	}

	tenths := int64(line[end-1] - '0')
	ones := int64(line[end-3] - '0')

	var temp int64
	var stationNameBytes []byte

	c := line[end-4]

	if c == ';' {
		temp = ones*10 + tenths
		stationNameBytes = line[:end-4]
	} else if c == '-' {
		temp = -(ones*10 + tenths)
		stationNameBytes = line[:end-5]
	} else {
		tens := int64(c - '0')
		if line[end-5] == ';' {
			temp = tens*100 + ones*10 + tenths
			stationNameBytes = line[:end-5]
		} else {
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

func main() {
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		// Start the CPU profiler
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		// Ensure it stops when main exits
		defer pprof.StopCPUProfile()
	}

	if flag.NArg() < 1 {
		log.Fatal("Usage: go run main.go <input_file_path>")
	}
	inputFilePath := flag.Arg(0)

	startTime := time.Now()

	fileStat, err := os.Stat(inputFilePath)
	if err != nil {
		log.Fatal(err)
	}
	fileSize := fileStat.Size()

	numWorkers := runtime.NumCPU()
	chunkSize := fileSize / int64(numWorkers)

	resultChan := make(chan *ChunkResult, numWorkers)
	var wg sync.WaitGroup

	fmt.Printf("Processing with %d workers...\n", numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = fileSize
		}

		wg.Add(1)
		go processChunk(inputFilePath, start, end, &wg, resultChan)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	finalItems := [TableSize]StationItem{}

	for chunk := range resultChan {
		for i := 0; i < TableSize; i++ {
			item := &chunk.Items[i]
			if item.Key == nil {
				continue
			}

			h := hash(item.Key)
			idx := h & (TableSize - 1)

			for {
				if finalItems[idx].Key == nil {
					finalItems[idx] = *item
					break
				}
				if bytes.Equal(finalItems[idx].Key, item.Key) {
					s := &finalItems[idx].Stats
					s.Sum += item.Stats.Sum
					s.Count += item.Stats.Count
					if item.Stats.Min < s.Min {
						s.Min = item.Stats.Min
					}
					if item.Stats.Max > s.Max {
						s.Max = item.Stats.Max
					}
					break
				}
				idx = (idx + 1) & (TableSize - 1)
			}
		}
	}

	var sortedStations []string
	results := make(map[string]*StationStats)

	for i := range finalItems {
		if finalItems[i].Key == nil {
			continue
		}
		name := string(finalItems[i].Key)
		sortedStations = append(sortedStations, name)
		results[name] = &finalItems[i].Stats
	}
	sort.Strings(sortedStations)

	outputFile, err := os.Create("output.txt")
	if err != nil {
		log.Fatal(err)
	}
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
	writer.Flush()

	duration := time.Since(startTime)
	fmt.Printf("Finished in %s\n", duration)
}
