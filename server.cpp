#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstring>
#include <csignal>
#include "dbms/working.h"

using namespace std;

int serverSocket;

void signalHandler(int signum) {
    cout << "\nСервер завершает работу..." << endl;
    if (serverSocket >= 0) {
        close(serverSocket);
        cout << "Серверный сокет закрыт." << endl;
    }
    exit(signum);
}

void handleClient(int clientSocket) {
    bool waiting = true;

    while(waiting){
        const int BUFFER_SIZE = 1024;
        char buffer[BUFFER_SIZE];

        ssize_t bytesRead = read(clientSocket, buffer, BUFFER_SIZE);
        if (bytesRead > 0) {
            buffer[bytesRead -1 ] = '\0';
            cout << "Получено сообщение: " << buffer << endl;

            string queury(buffer);

            if(queury == "END") waiting = false;

            string response = queue_work(queury);

            response += '\n';

            if (send(clientSocket, response.c_str(), response.size(), 0) < 0) {
                cerr << "Ошибка при отправке данных клиенту" << endl;
                waiting = false;
            }
        }
        else{
            cout << "Клиент закрыл соединение." << endl;
            waiting = false;
        }
    }

    close(clientSocket);
    cout << "Соединение закрыто." << endl;
}

int main() {
    const int PORT = 7432;

    struct sockaddr_in serverAddr;

    signal(SIGINT, signalHandler);

    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == 0) {
        perror("Ошибка при создании сокета");
        exit(EXIT_FAILURE);
    }

    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(PORT);

    if (bind(serverSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("Ошибка при привязке сокета");
        close(serverSocket);
        exit(EXIT_FAILURE);
    }

    if (listen(serverSocket, 10) < 0) {
        perror("Ошибка при прослушивании");
        close(serverSocket);
        exit(EXIT_FAILURE);
    }

    cout << "Сервер ожидает соединений на порту " << PORT << "..." << endl;

    while (true) {
        struct sockaddr_in clientAddr;
        socklen_t clientLen = sizeof(clientAddr);

        int clientSocket = accept(serverSocket, (struct sockaddr *)&clientAddr, &clientLen);
        if (clientSocket < 0) {
            perror("Ошибка при принятии соединения");
            continue;
        }

        cout << "Новое соединение принято." << endl;

        thread clientThread(handleClient, clientSocket);
        clientThread.detach();
    }

    close(serverSocket);
    return 0;
}
