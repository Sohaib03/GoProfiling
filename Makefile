INPUT_FILE ?= measurements.txt

all: run

run:
	go run main.go $(INPUT_FILE)

test:
	go run main.go samples/measurements-20.txt
	@diff -q output.txt samples/measurements-20.out && echo "success" || echo "failed"

test2:
	go run main.go samples/measurements-10000-unique-keys.txt
	@diff -q output.txt samples/measurements-10000-unique-keys.out && echo "success" || echo "failed"
clean:
	rm -f output.txt

.PHONY: all run clean test