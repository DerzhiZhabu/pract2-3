#include <string>
#include <iostream>
#include <filesystem>
#include <unistd.h>
#include <fstream>
#include <mutex>
#include "structures.h"
#include "json_parse.h"
#include "csv.h"
#include "db_working.h"


using namespace std;

string queue_work(string& data){
    HashTable<List<string>> tables;
    List<string> tables_names;
    string schem_name;
    int limit;

    getJSON(tables, tables_names, schem_name, limit);
    check_csv(schem_name, tables, tables_names);

    string res = console_parse(schem_name, tables, tables_names, limit, data);

    return res;
}

string console_parse(string& schem_name, HashTable<List<string>>& tables, List<string>& tables_names, int& limit, string data){
    filesystem::path dirPath = "dbms/.";
    string line = data;
    bool ended = true;
    try{
        string command;
        int i = 0;

        while(i < line.length()){

            while(line[i] != ' ' && i < line.length()){
                command.push_back(line[i]);
                i++;
            }
            i++;

            if (command == "SELECT"){
                return select(line, i, schem_name, tables);
            }
            else if(command == "INSERT"){
                string cm = take_string(line, i);
                cout << cm << endl;
                if (cm != "INTO") throw runtime_error("Wrong syntax");
                
                return insert(line, i, tables, schem_name, limit);
            }
            else if(command == "DELETE"){
                string cm = take_string(line, i);
                if (cm != "FROM") throw runtime_error("Wrong syntax");
                return deleting(line, i, tables, schem_name);
            }
            else if (command == "END"){
                ended = false;
                return "END";
            }
            else{
                throw runtime_error("Unknown command");
            }
        }
    }catch(runtime_error err){
        return err.what();
    }
    return ":)";
}

void check_csv(string& schem_name, HashTable<List<string>>& tables, List<string>& tables_names){
    filesystem::path dirPath = "dbms/.";
    dirPath = dirPath / schem_name;

    if (filesystem::exists(dirPath) &filesystem::is_directory(dirPath)) {
        return;
    } else {
        for(int i = 0; i < tables_names.length; i++){
            filesystem::create_directories(dirPath/tables_names[i]);
            List<List<string>> headers;

            headers.push(tables.Get(tables_names[i]));
            unlock(dirPath/schem_name/tables_names[i], tables_names[i]);
            writeToCsv(dirPath/tables_names[i]/"1.csv", headers, tables_names[i], schem_name);
            setPrimaryKeyFile(dirPath/tables_names[i], tables_names[i], 0);
            headers.clear();
        }
    }
}


void get_tables(string& line, int& i, List<string>& selected_tables){
    bool more = true;
    int count = 0;

    while(more ){
        string name;
        if (i > line.length()) throw runtime_error("Wrong syntax");

        while(line[i] != ' ' && line[i] != ',' && i < line.length()){
                name.push_back(line[i]);
                i++;
        }

        if(selected_tables.find(name) != -1) throw runtime_error("Wrong syntax");

        selected_tables.push(name);

        name = "";
        if (line[i] == ' ' || i >= line.length()) more = false;
        else i++;
        i++;
    }
}

void double_clear(List<List<string>>& formed){
    for(int o = 0; o < formed.length; o++){
        formed[o].clear();
    }
    formed.clear();
}

void cross_joint(List<List<string>>& itog, List<List<string>>& pred){
    if (pred.length == 0) return;

    List<List<string>> res;
    
    for(int i = 0; i < itog.length; i++){
        for(int j = 0; j < pred.length; j++){

            List<string> prom1;
            List<string> geted = itog[i];

            copyList(geted, prom1);

            for(int o = 0; o < pred[j].length; o++){
                prom1.push(pred[j][o]);
            }

            res.push(prom1);
        }
    }

    double_clear(itog);
    itog = res;
}

void printRows(List<List<string>>& to_print){
    for(int i = 0; i < to_print.length; i++){
        for(int j = 0; j < to_print[i].length;j++){
            cout << to_print[i][j] << ' ';
        }
        cout << endl;
    }
}

string printFilteredRows(List<List<string>>& to_print, List<bool>& cmp){
    string res;

    for(int i = 0; i < to_print.length; i++){
        if (!cmp[i]) continue;
        for(int j = 0; j < to_print[i].length;j++){
            res = res + to_print[i][j] + ' ';
        }
        res += '\n';
    }

    return res;
}

void form(string& schem_name, List<string>& from_tables, HashTable<List<string>>& rasp, List<string>& posl, List<List<string>>& old_formed, HashTable<List<string>>& tables){
    filesystem::path dirPath = "dbms/.";

    for(int k = 0; k < from_tables.length; k++){

        List<List<string>> formed;

        for (int j = 0; j < rasp.Get(from_tables[k]).length; j++){
            string to_posl = from_tables[k] + '.' + rasp.Get(from_tables[k])[j];
            posl.push(to_posl);
            
            List<List<string>> geted;

            readFromCsv(dirPath/schem_name/from_tables[k]/"1.csv", geted, from_tables[k], schem_name);

            int index = tables.Get(from_tables[k]).find(rasp.Get(from_tables[k])[j]);

            List<List<string>> newFromed;

            for (int o = 0; o < geted.length - 1; o++){
                List<string> formedRow;

                if (formed.length > o){
                    List<string> preFormed = formed[o];
                    copyList(preFormed, formedRow);
                }

                formedRow.push(geted[o + 1][index]);
                newFromed.push(formedRow);
            }

            double_clear(formed);
            formed = newFromed;

            double_clear(geted);
        }

        if(old_formed.length >= formed.length){
            cross_joint(old_formed, formed);
            double_clear(formed);
        }
        else{
            cross_joint(formed, old_formed);
            double_clear(old_formed);
            old_formed = formed;
        }
    }

}

void where_select(string& line, int& i, string& logi, List<string>& posl, List<List<string>>& old_formed, List<bool>& cmp){
    while (logi != ""){
            string first = take_string(line, i);
            string op = take_string(line, i);
            if (op != "=") throw runtime_error("Invalid operator");
            string second = take_string(line, i);

            if (posl.find(first) == -1) first = unwrap_string(first);
            if (posl.find(second) == -1) second = unwrap_string(second);

            int sod_first = posl.find(first);
            int sod_second = posl.find(second);

            List<bool> cmp_new;


            for(int j = 0; j < old_formed.length; j++){
                string sr_first = first;
                string sr_second = second;

                if (sod_first != -1) sr_first = old_formed[j][sod_first];
                if (sod_second != -1) sr_second = old_formed[j][sod_second];

                cmp_new.push(sr_first == sr_second);
            }
            
            List<bool> cmp_res;

            for (int j = 0; j < cmp.length; j++){
                if (logi == "AND") cmp_res.push(cmp[j] && cmp_new[j]);
                else if (logi == "OR") cmp_res.push(cmp[j] || cmp_new[j]);
                else throw runtime_error("Invalid logick");
            }
            
            cmp.clear();
            cmp_new.clear();
            cmp = cmp_res;

            logi = take_string(line, i);
        }
}

string select(string& line, int& i, string& schem_name, HashTable<List<string>>& tables){
    filesystem::path dirPath = "dbms/.";
    List<string> selected_tables;
    List<string> selected_columns;
    get_names(line, i, selected_tables, selected_columns);
    
    string com = take_string(line, i);
    if (com != "FROM") throw runtime_error("Wrong syntax");

    List<string> from_tables;
    get_tables(line, i, from_tables);

    if (!check_synt(selected_tables, from_tables)) throw runtime_error("Wrong syntax");

    HashTable<List<string>> rasp;

    for(int o = 0; o < from_tables.length; o++){
        List<string> forRasp;
        for(int j = 0; j < selected_tables.length; j++){
            if (selected_tables[j] == from_tables[o]){
                if (selected_columns[j] == "*"){
                    List<string> dop = tables.Get(selected_tables[j]);
                    copyList(dop, forRasp);
                    continue;
                }
                if (forRasp.find(selected_columns[j]) != -1) throw runtime_error("Wrong syntax");
                forRasp.push(selected_columns[j]);
            }
        }
        rasp.Add(from_tables[o], forRasp);
    }

    List<List<string>> old_formed;
    List<string> posl;

    form(schem_name, from_tables, rasp, posl, old_formed, tables);

    com = take_string(line, i);
    string logi = "AND";

    List<bool> cmp;
    for(int j = 0; j < old_formed.length; j++){
        cmp.push(true);
    }

    if (com == "WHERE"){
        where_select(line, i, logi, posl, old_formed, cmp);
    }

    string ress = printFilteredRows(old_formed, cmp);
    
    selected_tables.clear();
    selected_columns.clear();

    for (int h = 0; h < from_tables.length; h++){
        rasp.Get(from_tables[h]).clear();
    }

    from_tables.clear();
    posl.clear();
    double_clear(old_formed);

    return ress;
}

string take_list(string& cont, int& index){
    string content;
    bool get_content = 0;
    bool geting_content = 0;

    while (index < cont.length()){
        if (get_content) break;
        else if (!get_content){
            if (cont[index] == '(' || cont[index] == ')'){
                if (geting_content) get_content = true;
                else geting_content = true;
            }

            else if (geting_content) content.push_back(cont[index]);
        }

        index++;
    }
    return content;
}

string insert(string& line, int& i, HashTable<List<string>>& tables, string& schem_name, int& limit){
    filesystem::path dirPath = "dbms/.";
    string table_name = take_string(line, i);

    string cm = take_string(line, i);
    List<string> values;

    if (cm != "VALUES") throw runtime_error("Wrong syntax");

    string wraped = take_list(line, i);
    
    bool more = true;

    string value;
    int j = 0;

    while (more){
        if (j >= wraped.length()) more = false;
        if (wraped[j] == ','){
            value = unwrap_string(value);
            values.push(value);
            value = "";
            j++;
        }
        else value.push_back(wraped[j]);

        j++;
    }
    if(value != "") values.push(unwrap_string(value));

    if (tables.Get(table_name).length > values.length) throw runtime_error("Too few arguments");
    if (tables.Get(table_name).length < values.length) throw runtime_error("Too many argumets");

    int pk = readPrimaryKeyValue(dirPath/schem_name/table_name, table_name);
    if (pk + 1 > limit) throw runtime_error("Out of limit");

    appendToCsv(dirPath/schem_name/table_name/"1.csv", values, table_name, schem_name);
    setPrimaryKeyFile(dirPath/schem_name/table_name, table_name, pk+1);

    values.clear();

    return "Success";
}

string intToString(int number) {
    stringstream ss;
    ss << number;
    return ss.str();
}

string deleting(string& line, int& i, HashTable<List<string>>& tables, string& schem_name){

    filesystem::path dirPath = "dbms/.";

    List<List<string>> table;
    List<List<string>> new_table;
    List<string> headers;
    int counter = 0;

    string table_name = take_string(line, i);

    readFromCsv(dirPath/schem_name/table_name/"1.csv", new_table, table_name, schem_name);

    for(int i = 1; i < new_table.length; i++){
        List<string> tek = new_table[i];
        List<string> nov;
        copyList(tek, nov);
        table.push(nov);
    }

    List<string> newHeaders = new_table[0];

    for(int j = 0; j < newHeaders.length; j++){
            headers.push(table_name + '.' + newHeaders[j]);
    }
    double_clear(new_table);

    List<bool> cmp;
    
    for(int i = 0; i < table.length; i++){
        cmp.push(true);
    }

    string logi = "AND";
    
    if (take_string(line, i) == "WHERE") where_select(line, i, logi, headers, table, cmp);

    List<List<string>> result;
    List<string> origHead = tables.Get(table_name);
    List<string> headersDel;

    copyList(origHead, headersDel);

    result.push(headersDel);

    for(int i = 0; i < table.length; i++){
        if(!cmp[i]) result.push(table[i]);
        else counter++;
    }

    int ress = result.length;


    writeToCsv(dirPath/schem_name/table_name/"1.csv", result, table_name, schem_name);
    int pk = readPrimaryKeyValue(dirPath/schem_name/table_name, table_name);
    pk -= counter;
    setPrimaryKeyFile(dirPath/schem_name/table_name, table_name, pk);
    
    

    double_clear(table);
    headers.clear();

    return "Successfuly deleted " + intToString(ress) + "rows";
}

void copyList(List<string>& from, List<string>& to){
    for(int i = 0; i < from.length; i++){
        to.push(from[i]);
    }
}

string take_string(string& line, int& i){
    string res;
    while (line[i] != ' ' && i < line.length()){
        res.push_back(line[i]);
        i++;
    }
    i++;
    return res;
}

string unwrap_string(string str){
    string res;

    int index = 0;
    bool geting = false;

    while(index < str.length()){
        if (str[index] == '\''){
            if (geting) return res;
            else geting = true;
        }
        else if(geting) res.push_back(str[index]);

        index++;
    }

    throw runtime_error("Invalid argument");
}

void get_names(string& line, int& i, List<string>& selected_tables, List<string>& selected_columns){
    bool more = true;
    int count = 0;

    while(more ){
        string name;
        string column;
        bool geted_name = false;
        if (i > line.length()) throw runtime_error("Wrong syntax");

        while(line[i] != ' ' && line[i] != ',' && i < line.length()){
            if (line[i] == '.'){
                geted_name = true;
                i++;
            }
            if (!geted_name){
                name.push_back(line[i]);
            }
            else{
                column.push_back(line[i]);
            }
            i++;
        }

        selected_tables.push(name);
        selected_columns.push(column);

        name = "";
        column = "";
        geted_name = false;
        if (line[i] == ' ' || i >= line.length()) more = false;
        else i++;
        i++;
    }
}

bool check_synt(List<string>& selected_tables, List<string>& from_tables){
    for(int i = 0; i < selected_tables.length; i++){
        cout << from_tables[i] << endl;
        if (from_tables.find(selected_tables[i]) == -1) return false;
    }
    for(int i = 0; i < from_tables.length; i++){
        if (selected_tables.find(from_tables[i]) == -1) return false;
    }

    return true;
}

void setPrimaryKeyFile(const filesystem::path& directory, const std::string& tableName, int key) {
    
    filesystem::path filePath = directory / (tableName + "_pk");

    std::ofstream file(filePath, std::ios::out | std::ios::trunc);

    file << key;

    file.close();
}

int readPrimaryKeyValue(const filesystem::path& directory, const std::string& tableName) {

    filesystem::path filePath = directory / (tableName + "_pk");

    std::ifstream file(filePath);

    int value;
    file >> value;

    file.close();

    return value;
}