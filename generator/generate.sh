#!/bin/bash

# Array of values
values=(100000 500000 1000000 5000000 10000000 30000000)
# Array of names corresponding to the values
names=("100k" "500k" "1M" "5M" "10M" "30M")

# Length of the array
array_length=${#values[@]}

# Iterate through the arrays
for (( i=0; i<array_length; i++ ))
do
    value=${values[$i]}
    name=${names[$i]}
    # Execute the command with the current elements
    ./generator --puts $value --gets 0 --ranges 0 --deletes 0 --gets-misses-ratio 0.2 --gets-skewness 0.2 > ../dsl/$name.dsl 
done
