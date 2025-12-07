#include "Database.hpp"
#include <filesystem>

namespace docdb {

// Конструктор базы данных - инициализация таблиц из схемы
Database::Database(const Schema& schema) : schema_(schema) {
    initializeStorage();
    adt::Array table_names = schema_.getTableNames(); 
    for(size_t i = 0; i < table_names.size(); ++i) {
        const string& tableName = table_names.at(i);
        const auto& tableColumns = schema_.structure.at(tableName);

        TableConfig config;
        config.name = tableName;
        config.tuplesLimit = schema_.tuplesLimit;
        config.basePath = filesystem::path(schema_.name) / tableName;
        config.columns = tableColumns;
        tables_.insert(tableName, Table(config));
    }
}

Table& Database::getTable(const string& name) {
    if (!hasTable(name)) {
        throw runtime_error("Table not found: " + name);
    }
    Table* ptr = tables_.getPointer(name);
    if (ptr == nullptr) {
        throw runtime_error("Table not found: " + name);
    }
    return *ptr;
}

bool Database::hasTable(const string& name) const {
    return tables_.find(name);
}

adt::Array Database::getTableNames() const {
    return tables_.getAllKeys();
}

void Database::initializeStorage() {
    filesystem::create_directories(schema_.name);
}

string Database::getSchemaName() const {
    return schema_.name;
}

}
