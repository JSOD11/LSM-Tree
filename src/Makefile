CC=g++
CFLAGS=-Wall -Wextra -g

# Targets
all: server client

# Server
server: server.o
	$(CC) $(CFLAGS) -o server server.o

server.o: server.cpp
	$(CC) $(CFLAGS) -c server.cpp

# Client
client: client.o
	$(CC) $(CFLAGS) -o client client.o

client.o: client.cpp
	$(CC) $(CFLAGS) -c client.cpp

# Clean
clean:
	rm -f *.o server client
