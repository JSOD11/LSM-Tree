CC=g++ -std=c++17
CFLAGS=-Wall -Wextra -g -O3

# Targets
all: server 

# Server
server: server.o utils.o lsm.o bloomfilter.o MurmurHash3.o
	$(CC) $(CFLAGS) -o server server.o utils.o lsm.o bloomfilter.o MurmurHash3.o

server.o: server.cpp Utils.hpp lsm.hpp bloomfilter.hpp
	$(CC) $(CFLAGS) -c server.cpp

utils.o: utils.cpp Utils.hpp
	$(CC) $(CFLAGS) -c utils.cpp

lsm.o: Utils.hpp lsm.cpp lsm.hpp bloomfilter.hpp
	$(CC) $(CFLAGS) -c lsm.cpp

bloomfilter.o: Utils.hpp bloomfilter.cpp bloomfilter.hpp MurmurHash3.hpp
	$(CC) $(CFLAGS) -c bloomfilter.cpp

MurmurHash3.o: MurmurHash3.cpp MurmurHash3.hpp
	$(CC) $(CFLAGS) -c MurmurHash3.cpp

# Clean
clean:
	rm -rf *.o server client
cleandata:
	rm -rf data
