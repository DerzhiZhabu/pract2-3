#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include <thread>
#include <barrier>
#include <mutex>

using namespace std;

barrier b(4);

void client_function(const string& client_name) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        cerr << client_name << ": Ошибка создания сокета!" << endl;
        return;
    }

    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(7432);
    if (inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr) <= 0) {
        cerr << client_name << ": Ошибка преобразования IP-адреса!" << endl;
        close(sock);
        return;
    }


    if (connect(sock, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        cerr << client_name << ": Ошибка подключения к серверу!" << endl;
        close(sock);
        return;
    }

    cout << client_name << ": Подключение к серверу успешно установлено!" << endl;

    string message = "SELECT table1.* FROM table1\n";

    while (true) {
        b.arrive_and_wait();
        int bytesSent = send(sock, message.c_str(), message.length(), 0);
        if (bytesSent < 0) {
            cerr << client_name << ": Ошибка отправки данных!" << endl;
            break;
        }
        cout << client_name << ": Сообщение отправлено: " << message;

        sleep(1);
    }

    close(sock);
}

int main() {
    thread client1(client_function, "Клиент 1");
    thread client2(client_function, "Клиент 2");
    thread client3(client_function, "Клиент 3");
    thread client4(client_function, "Клиент 4");

    client1.join();
    client2.join();
    client3.join();
    client4.join();

    return 0;
}