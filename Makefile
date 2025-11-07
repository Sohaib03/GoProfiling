INPUT_FILE ?= measurements.txt

all: run

run:
	go run main.go $(INPUT_FILE)

test:
	go run main.go samples/measurements-20.txt
	@diff -q output.txt samples/measurements-20.out && echo "success" || echo "failed"

clean:
	rm -f output.txt

.PHONY: all run clean test