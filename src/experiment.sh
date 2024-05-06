#!/bin/bash

# Initialize arrays to store the times and page faults
declare -a put_runtimes
declare -a range_runtimes
declare -a page_faults

echo "size: "
read input
size=$input
# Repeat the sequence 5 times
for i in {1..5}
do
    echo "Execution $i:"
    
    # 1. Run ./server for put_runtime
    put_output=$(./server < ../dsl/$size.dsl)
    put_runtime=$(echo "$put_output" | grep -oP 'total runtime: \K\d+')
    put_runtimes+=($put_runtime)
    
    # 2. Run perf stat ./server for range_runtime and page faults
    perf_output=$(perf stat ./server < ../dsl/range.dsl 2>&1)
    range_runtime=$(echo "$perf_output" | grep -oP 'Range query took \K\d+')
    range_runtimes+=($range_runtime)
    fault=$(echo "$perf_output" | grep -oP '\s+\d+(,\d+)*\s+page-faults:u' | awk '{print $1}' | tr -d ',')
    page_faults+=($fault)
    
    # 3. Clean data
    make cleandata
done

# Print all collected runtimes and page faults
echo "$size,puts (ms)"
for runtime in "${put_runtimes[@]}"
do
    echo $runtime
done

echo "$size,range (ms)"
for runtime in "${range_runtimes[@]}"
do
    echo $runtime
done

echo "$size,range (page_faults)"
for fault in "${page_faults[@]}"
do
    echo $fault
done
