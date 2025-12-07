#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include <functional>
#include <nlohmann/json.hpp>

namespace docdb {

using namespace std;

struct TableConfig {
    string name;
    size_t tuplesLimit;
    filesystem::path basePath;
    vector<string> columns;
};

class Table {
public:
    explicit Table(const TableConfig & config);
    
    // Insert a new row. The values should correspond to the columns defined in schema (excluding PK).
    void insert(const vector<string>& values);
    
    // Delete rows that match the predicate.
    void deleteRows(const function<bool(const vector<string>& row, const vector<string>& columns)>& predicate);

    // Get all rows from the table.
    vector<vector<string>> scan();
    
    const vector<string>& getColumns() const;
    string getPkColumnName() const;

private:
    size_t getNextId();
    void lock();
    void unlock();
    
    // Helper to get all CSV files in order
    vector<filesystem::path> getDataFiles() const;
    filesystem::path getCurrentDataFilePath() const;
    size_t getCurrentFileRowCount() const;

    TableConfig config_;
    string pkColumnName_;
    filesystem::path pkSequenceFile_;
    filesystem::path lockFile_;
};

} 
