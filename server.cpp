#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstring>
#include <csignal> // Для обработки сигналов
#include "working.h"

// Глобальный дескриптор серверного сокета
int serverSocket;

// Обработчик сигнала завершения
void signalHandler(int signum) {
    std::cout << "\nСервер завершает работу..." << std::endl;
    if (serverSocket >= 0) {
        close(serverSocket); // Закрываем серверный сокет
        std::cout << "Серверный сокет закрыт." << std::endl;
    }
    exit(signum);
}

// Обработчик клиента
void handleClient(int clientSocket) {
    bool waiting = true;

    while(waiting){
        const int BUFFER_SIZE = 1024;
        char buffer[BUFFER_SIZE];

        // Чтение данных от клиента
        ssize_t bytesRead = read(clientSocket, buffer, BUFFER_SIZE);
        if (bytesRead > 0) {
            buffer[bytesRead -1 ] = '\0';
            std::cout << "Получено сообщение: " << buffer << std::endl;

            std::string queury(buffer);

            if(queury == "END") waiting = false;

            std::string response = queue_work(queury);

            response += '\n';

            // Отправка ответа
            send(clientSocket, response.c_str(), response.size(), 0);
        }
    }

    // Закрываем соединение
    close(clientSocket);
    std::cout << "Соединение закрыто." << std::endl;
}

int main() {
    const int PORT = 7432;

    struct sockaddr_in serverAddr;

    // Установка обработчика сигнала
    signal(SIGINT, signalHandler);

    // Создание сокета
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == 0) {
        perror("Ошибка при создании сокета");
        exit(EXIT_FAILURE);
    }

    // Настройка адреса сервера
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(PORT);

    // Привязка сокета к порту
    if (bind(serverSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("Ошибка при привязке сокета");
        close(serverSocket);
        exit(EXIT_FAILURE);
    }

    // Ожидание входящих подключений
    if (listen(serverSocket, 10) < 0) {
        perror("Ошибка при прослушивании");
        close(serverSocket);
        exit(EXIT_FAILURE);
    }

    std::cout << "Сервер ожидает соединений на порту " << PORT << "..." << std::endl;

    while (true) {
        struct sockaddr_in clientAddr;
        socklen_t clientLen = sizeof(clientAddr);

        // Принятие нового соединения
        int clientSocket = accept(serverSocket, (struct sockaddr *)&clientAddr, &clientLen);
        if (clientSocket < 0) {
            perror("Ошибка при принятии соединения");
            continue;
        }

        std::cout << "Новое соединение принято." << std::endl;

        // Запуск обработчика в отдельном потоке
        std::thread clientThread(handleClient, clientSocket);
        clientThread.detach(); // Отделяем поток, чтобы он мог работать независимо
    }

    // Закрываем серверный сокет
    close(serverSocket);
    return 0;
}
