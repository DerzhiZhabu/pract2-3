#ifndef DB_WORKING_H
#define DB_WORKING_H

#include <string>
#include <iostream>
#include <filesystem>
#include <unistd.h>
#include <fstream>
#include "structures.h"
#include "json_parse.h"
#include "csv.h"

using namespace std;


string console_parse(string& schem_name, HashTable<List<string>>& tables, List<string>& tables_names, int& limit, string data);
void check_csv(string& schem_name, HashTable<List<string>>& tables, List<string>& tables_names);

void printRows(List<List<string>>& to_print);
void copyList(List<string>& from, List<string>& to);
string take_string(string& line, int& i);
string unwrap_string(string str);
void get_names(string& line, int& i, List<string>& selected_tables, List<string>& selected_columns);
bool check_synt(List<string>& selected_tables, List<string>& from_tables);

string deleting(string& line, int& i, HashTable<List<string>>& tables, string& schem_name);
string insert(string& line, int& i, HashTable<List<string>>& tables, string& schem_name, int& limit);
string select(string& line, int& i, string& schem_name, HashTable<List<string>>& tables);
void where_select(string& line, int& i, string& logi, List<string>& posl, List<List<string>>& old_formed, List<bool>& cmp);
int readPrimaryKeyValue(const filesystem::path& directory, const std::string& tableName);
void setPrimaryKeyFile(const filesystem::path& directory, const std::string& tableName, int key);

#endif