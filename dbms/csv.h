#ifndef CSV_H
#define CSV_H

#include <string>
#include <filesystem>

void unlock(const filesystem::path& directory, const std::string tableName);
void writeToCsv(const filesystem::path& filename, List<List<string>>& data, string table_name, string& schem_name);
void readFromCsv(const filesystem::path& filename, List<List<string>>& data, string table_name, string& schem_name);
void appendToCsv(const filesystem::path& filename, List<string>& newRow, string table_name, string& schem_name);

#endif