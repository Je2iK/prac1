// include/Schema.hpp
#pragma once

#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <filesystem>
#include "../adt/ChainingHashTable.hpp"
#include "../adt/Array.hpp"

namespace docdb {

using namespace std;
using json = nlohmann::json;

struct Schema {
    string name;
    size_t tuplesLimit;
    adt::ChainingHashTable<string, vector<string>> structure; // Table name -> List of columns

    static Schema loadFromFile(const filesystem::path& path);
    adt::Array getTableNames() const;
};

}
