# Use the following makefile to build your project, for example:
# 1. To build the whole lab1 (builds mrapps, mrcoordinator, mrworker, mrsequential)
#    $ make lab1
# 2. To build the mrapps package (builds all the mrapps)
#    $ make build-mrapps
# 3. To build a specific mrapp, for exmaple wc:
#	 $ make build-mrapp-wc
# 4. To build the mr package
#    $ make build-mr

MRAPPS_SRC_DIR = src/mrapps
MAIN_SRC_DIR = src/main

# List of mrapps to build
MRAPPS = wc crash early_exit indexer jobcount mtiming nocrash rtiming

build-mrapps: $(addprefix build-mrapp-, $(MRAPPS))

build-mrapp-%:
	@echo "-- building $*"
	cd $(MRAPPS_SRC_DIR) && go build -buildmode=plugin $*.go

build-mr:
	@echo "-- building mrcoordinator mrworker mrsequential"
	cd $(MAIN_SRC_DIR) && go build mrcoordinator.go
	cd $(MAIN_SRC_DIR) && go build mrsequential.go
	cd $(MAIN_SRC_DIR) && go build mrworker.go

lab1: build-mrapps build-mr
	@echo "-- lab1 built successfully"

clean:
	rm -f $(MAIN_SRC_DIR)/mrcoordinator $(MAIN_SRC_DIR)/mrsequential $(MAIN_SRC_DIR)/mrworker
	rm -f $(MRAPPS_SRC_DIR)/*.so
