INPUT_FILE ?= measurements.txt

all: run

run:
	go run main.go $(INPUT_FILE)

clean:
	rm -f output.txt

.PHONY: all run clean

