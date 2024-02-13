#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

int main() {

    std::cout << "Starting up server...\n" << std::endl;

    // Create a socket
    int listening = socket(AF_INET, SOCK_STREAM, 0);

    // Bind the socket to an IP / port
    sockaddr_in hint;
    hint.sin_family = AF_INET;
    hint.sin_port = htons(6789); // Host TO Network Short
    hint.sin_addr.s_addr = INADDR_ANY; // Could also use inet_pton ...
    bind(listening, (sockaddr*)&hint, sizeof(hint));

    // Mark the socket for listening in
    listen(listening, SOMAXCONN);

    // Accept a call
    sockaddr_in client;
    socklen_t clientSize = sizeof(client);

    int clientSocket = accept(listening, (sockaddr*)&client, &clientSize);
    std::cout << "Connected to client. Server is listening...\n" << std::endl;

    // Close the listening socket
    close(listening);

    uint32_t length = 0;
    char buf[4096];
    while (true) {
        // Clear the buffer
        memset(buf, 0, 4096);

        // Wait for a message
        int sizeBytesReceived = recv(clientSocket, &length, sizeof(length), 0);

        if (sizeBytesReceived == 0) {
            std::cout << "The client disconnected." << std::endl;
            break;
        }

        length = ntohl(length);

        int bytesReceived = recv(clientSocket, buf, length, 0);
        
        
        // Display message
        std::cout << "Received: " << std::string(buf, 0, bytesReceived) << std::endl;
    }

    // Close the socket
    close(clientSocket);

    return 0;
}
