CC=g++ -std=c++17
CFLAGS=-Wall -Wextra -g

# Targets
all: server client

# Server
server: server.o db_manager.o
	$(CC) $(CFLAGS) -o server server.o db_manager.o

server.o: server.cpp
	$(CC) $(CFLAGS) -c server.cpp

db_manager.o: db_manager.cpp
	$(CC) $(CFLAGS) -c db_manager.cpp

# Client
client: client.o
	$(CC) $(CFLAGS) -o client client.o

client.o: client.cpp
	$(CC) $(CFLAGS) -c client.cpp

# Clean
clean:
	rm -f *.o server client
