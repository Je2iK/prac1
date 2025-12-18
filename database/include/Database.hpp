// include/Database.hpp
#pragma once

#include "Table.hpp"
#include "Schema.hpp"
#include "../adt/ChainingHashTable.hpp"

using namespace std;

class Database {
public:
    explicit Database(const Schema& schema);
    ~Database();
    Table& getTable(const string& name);
    bool hasTable(const string& name) const;
    Array<string> getTableNames() const;
    string getSchemaName() const;


private:
    void initializeStorage();
    void lock();
    void unlock();
    Schema schema;
    ChainingHashTable<string, Table> tables;
    filesystem::path lockFile;
}; 
