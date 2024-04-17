CC=g++ -std=c++17
CFLAGS=-Wall -Wextra -g

# Targets
all: server client

# Server
server: server.o db_manager.o bloomfilter.o
	$(CC) $(CFLAGS) -o server server.o db_manager.o bloomfilter.o

server.o: server.cpp db_manager.hpp bloomfilter.hpp
	$(CC) $(CFLAGS) -c server.cpp

db_manager.o: db_manager.cpp db_manager.hpp bloomfilter.hpp
	$(CC) $(CFLAGS) -c db_manager.cpp

bloomfilter.o: bloomfilter.cpp bloomfilter.hpp
	$(CC) $(CFLAGS) -c bloomfilter.cpp

# Client
client: client.o
	$(CC) $(CFLAGS) -o client client.o

client.o: client.cpp
	$(CC) $(CFLAGS) -c client.cpp

# Clean
clean:
	rm -f *.o server client
