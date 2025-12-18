#include "Database.hpp"
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>


Database::Database(const Schema& schema) : schema(schema) {
    initializeStorage();
    lockFile = filesystem::path(schema.name) / ".db_lock";
    lock();
    
    Array<string> table_names = schema.getTableNames(); 
    for(size_t i = 0; i < table_names.getSize(); ++i) {
        const string& tableName = table_names.at(i);
        const auto& tableColumns = schema.structure.at(tableName);

        TableConfig config;
        config.name = tableName;
        config.tuplesLimit = schema.tuplesLimit;
        config.basePath = filesystem::path(schema.name) / tableName;
        config.columns = tableColumns;
        tables.insert(tableName, Table(config));
    }
}

Database::~Database() {
    unlock();
}

void Database::lock() {
    int retries = 0;
    while (filesystem::exists(lockFile)) {
        this_thread::sleep_for(chrono::milliseconds(100));
        retries++;
        if (retries > 50) {
            throw runtime_error("Database is locked by another process");
        }
    }
    ofstream f(lockFile);
    f << "locked";
}

void Database::unlock() {
    if (filesystem::exists(lockFile)) {
        filesystem::remove(lockFile);
    }
}

Table& Database::getTable(const string& name) {
    if (!hasTable(name)) {
        throw runtime_error("Table not found: " + name);
    }
    Table* ptr = tables.getPointer(name);
    if (ptr == nullptr) {
        throw runtime_error("Table not found: " + name);
    }
    return *ptr;
}

bool Database::hasTable(const string& name) const {
    return tables.find(name);
}

Array<string> Database::getTableNames() const {
    return tables.getAllKeys();
}

void Database::initializeStorage() {
    filesystem::create_directories(schema.name);
}

string Database::getSchemaName() const {
    return schema.name;
}


