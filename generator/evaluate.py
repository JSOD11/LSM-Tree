# ================================================
# = Harvard University | CS265 | Systems Project =
# ================================================
# ========== LSM TREE WORKLOAD EVALUATOR =========
# ================================================
# Contact:
# ========
# - Kostas Zoumpatianos <kostas@seas.harvard.edu>
# - Michael Kester <kester@eecs.harvard.edu>

import sys
import time
import struct

# ------------------------------------------------
#                     Stats
# ------------------------------------------------
puts = 0
successful_gets = 0
failed_gets = 0
ranges = 0
successful_deletes = 0
failed_deletes = 0
loads = 0
time_elapsed = 0

# Logging events
def log(action, verbose):
    global puts
    global successful_gets
    global failed_gets
    global ranges
    global successful_deletes
    global failed_deletes
    global loads
    if action == "PUT":
        puts += 1
    elif action == "SUCCESSFUL_GET":
        successful_gets += 1
    elif action == "FAILED_GET":
        failed_gets += 1
    elif action == "RANGE":
        ranges += 1
    elif action == "SUCCESSFUL_DELETE":
        successful_deletes += 1
    elif action == "FAILED_DELETE":
        failed_deletes += 1
    elif action == "LOAD":
        loads += 1
    if verbose:
        print(action)

# Printing stats
def print_stats(time_elapsed):
    global puts
    global successful_gets
    global failed_gets
    global ranges
    global successful_deletes
    global failed_deletes
    global loads  
    print("------------------------------------")
    print("PUTS", puts)
    print("SUCCESFUL_GETS", successful_gets)
    print("FAILED_GETS", failed_gets)
    print("RANGES", ranges)
    print("SUCCESSFUL_DELS", successful_deletes)
    print("FAILED_DELS", failed_deletes)
    print("LOADS", loads)
    print("TIME_ELAPSED", time_elapsed)
    print("------------------------------------")

# ------------------------------------------------
#                   Main function
# ------------------------------------------------
if __name__ == "__main__":
    # Options
    verbose = False
    show_output = True

    # Initialize key-value store
    db = {}

    # Open file
    f = open(sys.argv[1], 'r')

    # Start reading workload
    start = time.time()
    for line in f:
        if line:
            # PUT
            if line[0] == "p":
                (key, val) = map(int, line.split(" ")[1:3])
                db[key] = val
                log("PUT", verbose)
            # GET
            elif line[0] == "g":
                key = int(line.split(" ")[1])
                val = db.get(key, None)
                if val is not None:
                    if show_output:
                        print(key, " -> ", val)
                    log("SUCCESSFUL_GET", verbose)
                else:
                    if show_output:
                        print(key, " -> ")
                    log("FAILED_GET", verbose)
            # RANGE
            elif line[0] == "r":
                (range_start, range_end) = map(int, line.split(" ")[1:3])
                valid_items = [(k, v) for k, v in db.items() if range_start <= k < range_end]
                valid_items.sort()  # Sort by key
                if show_output:
                    print(" ".join(f"{k}:{v}" for k, v in valid_items))
                log("RANGE", verbose)
            # DELETE
            elif line[0] == "d":
                key = int(line.split(" ")[1])
                if key in db:
                    del db[key]
                    log("SUCCESSFUL_DELETE", verbose)
                else:
                    log("FAILED_DELETE", verbose)
            # LOAD
            elif line[0] == "l":
                log("LOAD", verbose)
                filename = line[2:-1].strip()
                with open(filename, "rb") as load_file:
                    while True:
                        buf = load_file.read(4)
                        if not buf: break
                        key = struct.unpack('i', buf)[0]
                        buf = load_file.read(4)
                        if not buf: break
                        val = struct.unpack('i', buf)[0]
                        db[key] = val
                        log("PUT", verbose)
    # Done
    end = time.time()
    time_elapsed = (end-start)

    # Print stats
    if not verbose:
        print_stats(time_elapsed)

    # Closing file
    f.close()
