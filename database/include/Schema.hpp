// include/Schema.hpp
#pragma once

#include <../jsonpars-lib/json.hpp>
#include <string>
#include <filesystem>
#include "../adt/ChainingHashTable.hpp"
#include "../adt/Array.hpp"

using namespace std;
using json = nlohmann::json;

struct Schema {
    string name;
    size_t tuplesLimit;
    ChainingHashTable<string, Array<string>> structure;

    static Schema loadFromFile(const filesystem::path& path);
    Array<string> getTableNames() const;
};

