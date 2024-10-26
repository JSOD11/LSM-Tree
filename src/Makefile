CC=g++ -std=c++17
CFLAGS=-Wall -Wextra -g

all: server 

server: server.o MurmurHash3.o
	$(CC) $(CFLAGS) -o server server.o MurmurHash3.o

server.o: server.cpp Utils.hpp lsm.hpp bloomfilter.hpp
	$(CC) $(CFLAGS) -c server.cpp

MurmurHash3.o: MurmurHash3.cpp MurmurHash3.hpp
	$(CC) $(CFLAGS) -c MurmurHash3.cpp

clean:
	rm -rf *.o server client data
cleandata:
	rm -rf data
