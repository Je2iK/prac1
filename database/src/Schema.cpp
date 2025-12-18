#include "Schema.hpp"
#include <fstream>


Schema Schema::loadFromFile(const filesystem::path& path) {
    ifstream f(path);
    if (!f.is_open()) {
        throw runtime_error("Cannot open schema.json");
    }
    json j;
    f >> j;
    Schema s;
    s.name = j.at("name").get<string>();
    s.tuplesLimit = j.at("tuples_limit").get<size_t>();
    json structure_json = j.at("structure");
    if (!structure_json.is_object()) {
        throw runtime_error("Structure in schema must be an object");
    }
    
    for (auto& [table_name, table_schema] : structure_json.items()) {
        Array<string> columns;
        if (table_schema.is_array()) {
            for (const auto& col : table_schema) {
                columns.append(col.get<string>());
            }
        }
        s.structure.insert(table_name, columns);
    }
    return s;
}

Array<string> Schema::getTableNames() const {
    return structure.getAllKeys();
} 
