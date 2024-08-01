#!/bin/bash

# Array of values.
values=(1000 10000 50000 100000 500000 1000000)
# Array of names corresponding to the values.
names=("1k" "10k" "50k" "100k" "500k" "1m")

array_length=${#values[@]}

# Ensure the output directory exists.
output_dir="../dsl/dict"
mkdir -p "$output_dir"

for (( i=0; i<array_length; i++ ))
do
    value=${values[$i]}
    name=${names[$i]}
    puts=$((value/2))
    gets=$((value/4))
    ranges=$((value/4))
    deletes=$((value/4))
    ./generator --puts $puts --gets $gets --ranges $ranges --deletes $deletes --gets-misses-ratio 0.2 --gets-skewness 0.2 > "$output_dir/$name.dsl"
done
