#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <filesystem>
#include <thread>
#include <chrono>
#include "structures.h"
#include <mutex>

mutex a;

using namespace std;


void lock(const filesystem::path& directory, const string tableName) {
    
    filesystem::path filePath = "dbms" / directory / (tableName + "_lock");

    ofstream file(filePath, ios::out | ios::trunc);

    file << 0;

    file.close();
}

void unlock(const filesystem::path& directory, const string tableName) {
    
    filesystem::path filePath = "dbms" / directory / (tableName + "_lock");

    ofstream file(filePath, ios::out | ios::trunc);

    file << 1;

    file.close();
}

int isunlocked(const filesystem::path& directory, const string& tableName) {
    if (!a.try_lock()) {
            cout <<"Thread is waiting for mutex..." << endl;
            this_thread::sleep_for(chrono::seconds(1));
    }
    else{
        a.unlock();
    }
    lock_guard<mutex> lock(a);

    filesystem::path filePath = "dbms" / directory / (tableName + "_lock");

    ifstream file(filePath);

    
    int value = 1;
    file >> value;


    file.close();

    return value;
}

void writeToCsv(const filesystem::path& filename, List<List<string>>& data, string table_name, string& schem_name) {
    filesystem::path filePath = "dbms/.";
    while(!isunlocked(filePath/schem_name/table_name, table_name)){
        this_thread::sleep_for(chrono::seconds(1));
        cout << "waiting" << endl;
    }
    lock(filePath/ schem_name/table_name, table_name);

    ofstream file(filename, ios::out | ios::trunc);

    for (int i = 0; i < data.length; i++) {
        for (size_t j = 0; j < data[i].length; j++) {
            file << data[i][j];
            if (j < data[i].length - 1) {
                file << ",";
            }
        }
        file << "\n";
    }

    file.close();
    unlock(filePath/ schem_name/table_name, table_name);
}

void readFromCsv(const filesystem::path& filename, List<List<string>>& data, string table_name, string& schem_name) {

    filesystem::path filePath = "dbms/.";
    while(!isunlocked(filePath/schem_name/table_name, table_name)){
        this_thread::sleep_for(chrono::seconds(1));
        cout << "waiting" << endl;
    }
    lock(filePath/ schem_name/table_name, table_name);


    ifstream file(filename);
    string line;

    while (getline(file, line)) {
        List<string> row;
        string cell;
        istringstream lineStream(line);

        while (getline(lineStream, cell, ',')) {
            row.push(cell);
        }

        data.push(row);
    }

    file.close();
    unlock(filePath/ schem_name/table_name, table_name);
}


void appendToCsv(const filesystem::path& filename, List<string>& newRow, string table_name, string& schem_name) {

    filesystem::path filePath = "dbms/.";
    while(!isunlocked(filePath/schem_name/table_name, table_name)){
        this_thread::sleep_for(chrono::seconds(1));
        cout << "waiting" << endl;
    }
    lock(filePath/ schem_name/table_name, table_name);
    
    ofstream file(filename, ios::app);

    for (size_t i = 0; i < newRow.length; ++i) {
        file << newRow[i];
        if (i < newRow.length - 1) {
            file << ",";
        }
    }
    file << "\n";

    file.close();
    unlock(filePath/ schem_name/table_name, table_name);
}