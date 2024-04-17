# LSM-Tree

A Log-Structured Merge Tree implementation. At the moment, the implementation of the tree is a leveled LSM tree, with an unsorted buffer. There are a number of projects to complete at the current moment, outlined in `Roadmap.md`.

# Usage

First

```
cd src
```

Then

```
make
```

## Server
To start the server, run
```
./server
```

## Client
In a separate terminal, run
```
./client
```
This will connect the client to the server. The following commands are currently supported in the client:

```
p x y — PUT
g x   — GET
p     — Print levels to server.
pv    — Print levels to server (verbose).
s     — Shutdown and persist.
sw    — Shutdown and wipe all data.
```

For example,

```
p 35 42
```

will put the key-value pair 35 -> 42 into the tree, and then

```
g 35
```

will return 42. Typing `p` will print out the general structure of the levels of the tree, while
`pv` will print out this same structure as well as all the fence pointers and key-value pairs. `s` shuts down the client - server connection, persists all data on the server, and terminates the client. `sw` has the same functionality as `s` but also wipes all the data from the server.

To batch load a larger number of commands into the client all at once, there are scripts in the
`dsl` folder. For example, try

```
./client < ../dsl/10k.dsl
```

to execute all the commands in the `/dsl/10k.dsl` file all at once.

### Getting Started

In `lsm.hpp`, change `BUFFER_SIZE` to be some small number like 3 and then execute 
some PUT and GET calls on a very small tree. Then use the `pv` command to get a sense
of how the key-value pairs, fence pointers, and bloom filters are stored.

### Testing

In the `generator` folder, there is a Python script called `evaluate.py` which can be called as follows:

```
python3 evaluate.py ../dsl/10k.dsl
```

Run the LSM tree on the same set of commands to verify that the two have the same output. The hashing solution will run much faster than the LSM Tree, but I believe this can be attributed to network delay. Within `db_manager.cpp`, there are versions of `put()` and `get()` that use the C++ standard map as the internal key-value store instead of the LSM Tree. Uncommenting these functions and commenting out the LSM versions shows that the LSM Tree seems to perform comparably to the C++ standard map.
