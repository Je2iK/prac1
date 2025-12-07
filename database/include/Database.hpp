// include/Database.hpp
#pragma once

#include "Table.hpp"
#include "Schema.hpp"
#include <string>
#include "../adt/ChainingHashTable.hpp"
#include <nlohmann/json.hpp>

namespace docdb {

using namespace std;
using json = nlohmann::json;

class Database {
public:
    explicit Database(const Schema& schema);
    Table& getTable(const string& name);
    bool hasTable(const string& name) const;
    adt::Array getTableNames() const;
    string getSchemaName() const;


private:
    void initializeStorage();
    Schema schema_; // Объект схемы, используемой базой данных
    adt::ChainingHashTable<string, Table> tables_;
};

} 
