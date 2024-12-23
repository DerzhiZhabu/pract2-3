#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <filesystem>
#include <thread>
#include <chrono>
#include "structures.h"


void lock(const filesystem::path& directory, const std::string tableName) {
    
    filesystem::path filePath = directory / (tableName + "_lock");

    std::ofstream file(filePath, std::ios::out | std::ios::trunc);

    file << 0;

    file.close();
}

void unlock(const filesystem::path& directory, const std::string tableName) {
    
    filesystem::path filePath = directory / (tableName + "_lock");

    std::ofstream file(filePath, std::ios::out | std::ios::trunc);

    file << 1;

    file.close();
}

int isunlocked(const filesystem::path& directory, const std::string& tableName) {

    filesystem::path filePath = directory / (tableName + "_lock");

    std::ifstream file(filePath);

    
    int value = 1;
    file >> value;


    file.close();

    return value;
}

void writeToCsv(const filesystem::path& filename, List<List<string>>& data, string table_name, string& schem_name) {
    filesystem::path filePath = ".";
    while(!isunlocked(filePath/schem_name/table_name, table_name)){
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cout << "waiting" << endl;
    }
    lock(filePath/ schem_name/table_name, table_name);

    std::ofstream file(filename, std::ios::out | std::ios::trunc);

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

    filesystem::path filePath = ".";
    while(!isunlocked(filePath/schem_name/table_name, table_name)){
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cout << "waiting" << std::endl;
    }
    lock(filePath/ schem_name/table_name, table_name);


    std::ifstream file(filename);
    std::string line;

    while (std::getline(file, line)) {
        List<string> row;
        std::string cell;
        std::istringstream lineStream(line);

        while (std::getline(lineStream, cell, ',')) {
            row.push(cell);
        }

        data.push(row);
    }

    file.close();
    unlock(filePath/ schem_name/table_name, table_name);
}


void appendToCsv(const filesystem::path& filename, List<string>& newRow, string table_name, string& schem_name) {

    filesystem::path filePath = ".";
    while(!isunlocked(filePath/schem_name/table_name, table_name)){
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cout << "waiting" << std::endl;
    }
    lock(filePath/ schem_name/table_name, table_name);
    
    std::ofstream file(filename, std::ios::app);

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