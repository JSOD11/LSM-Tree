#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <tuple>
#include <chrono>
#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>

#include "lsm.hpp"
#include "db_manager.hpp"

void populateCatalog(void) {
    // Create the data folder if it does not exist.
    if (!std::filesystem::exists("data")) std::filesystem::create_directory("data");

    // For now, we always begin with only l0 (the buffer). We will implement persistence later.
    int fd = open("data/l0.data", O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    size_t fileSize = 2 * catalog.bufferSize * sizeof(int);
    ftruncate(fd, fileSize);
    void* levelPointer = mmap(nullptr, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    catalog.levels[0] = reinterpret_cast<int*>(levelPointer);

    catalog.numLevels++;
}

int main() {

    std::cout << "\nStarting up server...\n" << std::endl;

    populateCatalog();

    int listening = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in hint;
    hint.sin_family = AF_INET;
    hint.sin_port = htons(PORT);
    hint.sin_addr.s_addr = INADDR_ANY;
    bind(listening, (sockaddr*)&hint, sizeof(hint));
    listen(listening, SOMAXCONN);
    sockaddr_in client;
    socklen_t clientSize = sizeof(client);
    int clientSocket = accept(listening, (sockaddr*)&client, &clientSize);
    close(listening);

    std::cout << "Connected to client. Server is listening...\n" << std::endl;

    uint32_t length = 0;
    char buf[4096];
    while (true) {
        // Clear the buffer.
        memset(buf, 0, 4096);

        // The first message describes the length of the client command.
        int sizeBytesReceived = recv(clientSocket, &length, sizeof(length), 0);

        if (sizeBytesReceived == 0) {
            std::cout << "\nThe client disconnected.\n" << std::endl;
            break;
        }

        length = ntohl(length);

        // This message contains the client's actual command.
        int bytesReceived = recv(clientSocket, buf, length, 0);

        std::string userCommand(buf, buf + bytesReceived);
        
        // Log the message.
        // std::cout << "Client command: " << userCommand << std::endl;

        // Process the message and send a reply to the client.
        std::string replyMessage;
        Status status;
        std::tie(status, replyMessage) = processCommand(userCommand);

        Message* message = new Message();
        message->status = status;
        strncpy(message->message, replyMessage.c_str(), sizeof(message->message) - 1);
        message->message[sizeof(message->message) - 1] = '\0';
        message->messageLength = strlen(message->message);

        send(clientSocket, message, sizeof(Message), 0);

        delete message;
    }

    close(clientSocket);

    return 0;
}
