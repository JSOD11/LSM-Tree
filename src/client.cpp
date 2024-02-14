#include <iostream>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "lsm.hpp"

int main() {

    std::cout << "\nStarting up client...\n" << std::endl;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        std::cout << "Could not connect to server." << std::endl;
        return 1;
    }
    std::string ipAddress = "0.0.0.0";
    sockaddr_in hint;
    hint.sin_family = AF_INET;
    hint.sin_port = htons(PORT);
    inet_pton(AF_INET, ipAddress.c_str(), &hint.sin_addr);
    int connectRes = connect(sock, (sockaddr*)&hint, sizeof(hint));
    if (connectRes == -1) {
        std::cout << "Could not connect to server." << std::endl;
        return 1;
    }

    std::string userInput;
    std::cout << "> ";
    auto start = std::chrono::high_resolution_clock::now();
    while (std::getline(std::cin, userInput)) {
        if (!userInput.empty()) {

            // Send the length of the message.
            uint32_t length = htonl(userInput.size());
            send(sock, &length, sizeof(length), 0);

            // Send the message.
            send(sock, userInput.c_str(), userInput.size(), 0);

            // Receive the server's response.
            Message* replyMessage = new Message();
            recv(sock, replyMessage, sizeof(Message), 0);

            // Notify the client if the server returns an error.
            if (replyMessage->status != SUCCESS) {
                std::cout << "The server returned an error." << std::endl;
            }

            // Output the server's response.
            std::string payload(replyMessage->message);
            if (payload != "") {
                std::cout << replyMessage->message << std::endl;
            }
            
            delete replyMessage;

            if (userInput == "s" || userInput == "shutdown" || userInput == "sw") {
                std::cout << "\nShutting down client.\n" << std::endl;
                break;
            }
            std::cout << "> ";
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();

    std::cout << "Connection duration: " << duration << " s." << std::endl;

    close(sock);

    return 0;
}
