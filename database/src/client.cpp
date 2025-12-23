#include <iostream>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

using namespace std;

int main() {
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (clientSocket < 0) {
        cerr << "ошибка создания сокета\n";
        return 1;
    }
    
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(7432);
    inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);
    
    if (connect(clientSocket, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        cerr << "коннекту плоховато\n";
        close(clientSocket);
        return 1;
    }
    
    cout << "подключено к серверу на порту 7432\n";
    cout << "'quit' отключиться\n\n";
    
    string query;
    char buffer[4096];
    
    while (true) {
        cout << "db> ";
        if (!getline(cin, query)) break;
        
        if (query.empty()) continue;
        
        send(clientSocket, query.c_str(), query.length(), 0);
        
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytesReceived = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        
        if (bytesReceived > 0) {
            cout << buffer;
            if (string(buffer).find("пока") != string::npos) {
                break;
            }
        }
    }
    
    close(clientSocket);
    return 0;
}
