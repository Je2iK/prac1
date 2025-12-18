#pragma once

#include <string>
#include <filesystem>
#include <functional>
#include "../adt/Array.hpp"

using namespace std;

struct TableConfig {
    string name;
    size_t tuplesLimit;
    filesystem::path basePath;
    Array<string> columns;
};

class Table {
public:
    explicit Table(const TableConfig & config);
    
    void insert(const Array<string>& values);
    
    void deleteRows(const function<bool(const Array<string>& row, const Array<string>& columns)>& predicate);

    Array<Array<string>> scan();
    
    const Array<string>& getColumns() const;
    string getPkColumnName() const;

private:
    size_t getNextId();
    void lock();
    void unlock();
    
    Array<filesystem::path> getDataFiles() const;
    filesystem::path getCurrentDataFilePath() const;
    size_t getCurrentFileRowCount() const;

    TableConfig config;
    string pkColumnName;
    filesystem::path pkSequenceFile;
    filesystem::path lockFile;
}; 
