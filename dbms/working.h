#include <string>
#include <iostream>
#include <filesystem>
#include <unistd.h>
#include <fstream>
#include "structures.h"
#include "json_parse.h"
#include "csv.h"

using namespace std;

string queue_work(string& data);