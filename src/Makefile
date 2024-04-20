CC=g++ -std=c++17
CFLAGS=-Wall -Wextra -g

# Targets
all: server client

# Server
server: server.o utils.o LSM.o bloomfilter.o MurmurHash3.o
	$(CC) $(CFLAGS) -o server server.o utils.o LSM.o bloomfilter.o MurmurHash3.o

server.o: server.cpp Utils.hpp LSM.hpp bloomfilter.hpp 
	$(CC) $(CFLAGS) -c server.cpp

utils.o: utils.cpp Utils.hpp
	$(CC) $(CFLAGS) -c utils.cpp

LSM.o: Utils.hpp LSM.cpp LSM.hpp bloomfilter.hpp
	$(CC) $(CFLAGS) -c LSM.cpp

bloomfilter.o: Utils.hpp bloomfilter.cpp bloomfilter.hpp MurmurHash3.hpp
	$(CC) $(CFLAGS) -c bloomfilter.cpp

MurmurHash3.o: MurmurHash3.cpp MurmurHash3.hpp
	$(CC) $(CFLAGS) -c MurmurHash3.cpp

# Client
client: client.o utils.o
	$(CC) $(CFLAGS) -o client client.o utils.o

client.o: client.cpp Utils.hpp
	$(CC) $(CFLAGS) -c client.cpp

# Clean
clean:
	rm -rf *.o server client data
