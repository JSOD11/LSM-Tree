#include <iostream>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

int main() {
    // Create a socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        return 1;
    }

    // Create a hint structure for the server we're connecting with
    int port = 6789;
    std::string ipAddress = "0.0.0.0";

    sockaddr_in hint;
    hint.sin_family = AF_INET;
    hint.sin_port = htons(port);
    inet_pton(AF_INET, ipAddress.c_str(), &hint.sin_addr);

    // Connect to the server on the socket
    int connectRes = connect(sock, (sockaddr*)&hint, sizeof(hint));
    if (connectRes == -1) {
        return 1;
    }

    std::string userInput;
    std::cout << "> ";
    while (std::getline(std::cin, userInput)) {
        if (!userInput.empty()) {
            // Send length of message.
            uint32_t length = htonl(userInput.size());
            send(sock, &length, sizeof(length), 0);

            // Send message.
            send(sock, userInput.c_str(), userInput.size(), 0);

            if (userInput == "shutdown") {
                break;
            }
            std::cout << "> ";
        }
    }

    // Close the socket
    close(sock);

    return 0;
}
