#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include <thread>  // Для std::thread

// Функция для работы клиента
void client_function(const std::string& client_name) {
    // Создание сокета
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        std::cerr << client_name << ": Ошибка создания сокета!" << std::endl;
        return;
    }

    // Установка адреса сервера
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(7432);
    if (inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr) <= 0) {
        std::cerr << client_name << ": Ошибка преобразования IP-адреса!" << std::endl;
        close(sock);
        return;
    }


    if (connect(sock, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        std::cerr << client_name << ": Ошибка подключения к серверу!" << std::endl;
        close(sock);
        return;
    }

    std::cout << client_name << ": Подключение к серверу успешно установлено!" << std::endl;

    std::string message = "SELECT table1.* FROM table1\n";

    while (true) {
        int bytesSent = send(sock, message.c_str(), message.length(), 0);
        if (bytesSent < 0) {
            std::cerr << client_name << ": Ошибка отправки данных!" << std::endl;
            break;
        }
        std::cout << client_name << ": Сообщение отправлено: " << message;

        sleep(1);
    }

    close(sock);
}

int main() {
    // Создаем два потока для двух клиентов
    std::thread client1(client_function, "Клиент 1");
    std::thread client2(client_function, "Клиент 2");

    // Ожидаем завершения работы потоков
    client1.join();
    client2.join();

    return 0;
}