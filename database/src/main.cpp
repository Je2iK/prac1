#include "Database.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <regex>
#include <map>
#include <algorithm>
#include <functional>
#include <filesystem>
#include <fstream>

using namespace std;
using namespace docdb;

// Токенизатор SQL команд
vector<string> tokenize(const string& query) {
    vector<string> tokens;
    string current;
    bool inQuote = false;
    for (size_t i = 0; i < query.length(); ++i) {
        char c = query[i];
        if (inQuote) {
            if (c == '\'') {
                inQuote = false;
                current += c;
                tokens.push_back(current);
                current = "";
            } else {
                current += c;
            }
        } else {
            if (isspace(c)) {
                if (!current.empty()) {
                    tokens.push_back(current);
                    current = "";
                }
            } else if (c == ',' || c == '=' || c == '(' || c == ')') {
                if (!current.empty()) {
                    tokens.push_back(current);
                    current = "";
                }
                tokens.push_back(string(1, c));
            } else if (c == '\'') {
                if (!current.empty()) {
                     tokens.push_back(current);
                     current = "";
                }
                inQuote = true;
                current += c;
            } else {
                current += c;
            }
        }
    }
    if (!current.empty()) tokens.push_back(current);
    return tokens;
}

// Удаление кавычек из строки
string stripQuotes(const string& s) {
    if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

struct Condition {
    string lhs;
    string rhs;
};

// Рекурсивный вычислитель выражений WHERE
bool evaluateExpression(const vector<string>& tokens, size_t& pos, const map<string, string>& rowData);

string getOperandValue(const string& operand, const map<string, string>& rowData) {
    if (operand.size() >= 2 && operand.front() == '\'' && operand.back() == '\'') {
        return stripQuotes(operand);
    }
    if (rowData.count(operand)) {
        return rowData.at(operand);
    }
    return operand; 
}

bool evaluateCondition(const vector<string>& tokens, size_t& pos, const map<string, string>& rowData) {
    if (pos >= tokens.size()) return false;
    string lhs = tokens[pos++];
    if (pos >= tokens.size() || tokens[pos] != "=") return false;
    pos++;
    if (pos >= tokens.size()) return false;
    string rhs = tokens[pos++];
    
    string lVal = getOperandValue(lhs, rowData);
    string rVal = getOperandValue(rhs, rowData);
    return lVal == rVal;
}

bool evaluateFactor(const vector<string>& tokens, size_t& pos, const map<string, string>& rowData) {
    if (pos >= tokens.size()) return false;
    if (tokens[pos] == "(") {
        pos++;
        bool result = evaluateExpression(tokens, pos, rowData);
        if (pos < tokens.size() && tokens[pos] == ")") pos++;
        return result;
    }
    return evaluateCondition(tokens, pos, rowData);
}

bool evaluateTerm(const vector<string>& tokens, size_t& pos, const map<string, string>& rowData) {
    bool left = evaluateFactor(tokens, pos, rowData);
    while (pos < tokens.size() && tokens[pos] == "AND") {
        pos++;
        bool right = evaluateFactor(tokens, pos, rowData);
        left = left && right;
    }
    return left;
}

bool evaluateExpression(const vector<string>& tokens, size_t& pos, const map<string, string>& rowData) {
    bool left = evaluateTerm(tokens, pos, rowData);
    while (pos < tokens.size() && tokens[pos] == "OR") {
        pos++;
        bool right = evaluateTerm(tokens, pos, rowData);
        left = left || right;
    }
    return left;
}

// Обработка SELECT запроса
void processSelect(const vector<string>& tokens, Database& db) {
    size_t pos = 1;
    vector<string> projection;
    while (pos < tokens.size() && tokens[pos] != "FROM") {
        if (tokens[pos] != ",") {
            projection.push_back(tokens[pos]);
        }
        pos++;
    }
    
    if (pos >= tokens.size() || tokens[pos] != "FROM") {
        cout << "Error: Expected FROM\n";
        return;
    }
    pos++;
    
    vector<string> tableNames;
    while (pos < tokens.size() && tokens[pos] != "WHERE") {
        if (tokens[pos] != ",") {
            tableNames.push_back(tokens[pos]);
        }
        pos++;
    }
    
    vector<string> whereTokens;
    if (pos < tokens.size() && tokens[pos] == "WHERE") {
        pos++;
        while (pos < tokens.size()) {
            whereTokens.push_back(tokens[pos++]);
        }
    }
    
    // Проверка существования таблиц и получение метаданных
    struct TableInfo {
        string name;
        vector<string> columns;
        string pkName;
        vector<filesystem::path> files;
    };
    
    vector<TableInfo> tables;
    for (const auto& tName : tableNames) {
        if (!db.hasTable(tName)) {
            cout << "Error: Table " << tName << " not found\n";
            return;
        }
        Table& table = db.getTable(tName);
        TableInfo tInfo;
        tInfo.name = tName;
        tInfo.columns = table.getColumns();
        tInfo.pkName = table.getPkColumnName();
        
        // Получение списка CSV файлов таблицы
        filesystem::path basePath = filesystem::path(db.getSchemaName()) / tName;
        if (filesystem::exists(basePath)) {
            for (const auto& entry : filesystem::directory_iterator(basePath)) {
                if (entry.path().extension() == ".csv") {
                    tInfo.files.push_back(entry.path());
                }
            }
            sort(tInfo.files.begin(), tInfo.files.end(), [](const filesystem::path& a, const filesystem::path& b) {
                try {
                    return stoi(a.stem().string()) < stoi(b.stem().string());
                } catch (...) {
                    return a.stem().string() < b.stem().string();
                }
            });
        }
        tables.push_back(tInfo);
    }
    
    // Парсинг строки CSV
    auto parseCsvLine = [](const string& line) -> vector<string> {
        vector<string> result;
        stringstream ss(line);
        string cell;
        while (getline(ss, cell, ',')) {
            result.push_back(cell);
        }
        return result;
    };
    
    // Создание мапы строки из CSV
    auto createRowMap = [](const vector<string>& row, const TableInfo& tInfo) -> map<string, string> {
        map<string, string> rowMap;
        if (row.size() > 0) rowMap[tInfo.pkName] = row[0];
        for (size_t i = 0; i < tInfo.columns.size(); ++i) {
            if (i + 1 < row.size()) {
                rowMap[tInfo.name + "." + tInfo.columns[i]] = row[i + 1];
            }
        }
        return rowMap;
    };
    
    // Рекурсивное вычисление картезианского произведения
    function<void(size_t, map<string, string>)> processCartesian;
    processCartesian = [&](size_t tableIdx, map<string, string> currentRow) {
        if (tableIdx >= tables.size()) {
            bool match = true;
            if (!whereTokens.empty()) {
                size_t p = 0;
                match = evaluateExpression(whereTokens, p, currentRow);
            }
            
            if (match) {
                for (size_t i = 0; i < projection.size(); ++i) {
                    if (i > 0) cout << ",";
                    if (currentRow.count(projection[i])) {
                        cout << currentRow.at(projection[i]);
                    } else {
                        cout << "NULL";
                    }
                }
                cout << "\n";
            }
            return;
        }
        
        // Последовательное чтение файлов таблицы
        const TableInfo& tInfo = tables[tableIdx];
        for (const auto& file : tInfo.files) {
            ifstream f(file);
            string line;
            bool header = true;
            
            while (getline(f, line)) {
                if (line.empty()) continue;
                if (header) {
                    header = false;
                    continue;
                }
                
                auto row = parseCsvLine(line);
                auto rowMap = createRowMap(row, tInfo);
                
                map<string, string> combined = currentRow;
                combined.insert(rowMap.begin(), rowMap.end());
                
                processCartesian(tableIdx + 1, combined);
            }
        }
    };
    
    processCartesian(0, map<string, string>());
}

// Обработка INSERT запроса
void processInsert(const vector<string>& tokens, Database& db) {
    if (tokens.size() < 6 || tokens[1] != "INTO" || tokens[3] != "VALUES") {
        cout << "Error: Invalid INSERT syntax\n";
        return;
    }
    string tableName = tokens[2];
    if (!db.hasTable(tableName)) {
        cout << "Error: Table " << tableName << " not found\n";
        return;
    }
    
    vector<string> values;
    size_t pos = 5;
    while (pos < tokens.size() && tokens[pos] != ")") {
        if (tokens[pos] != ",") {
            values.push_back(stripQuotes(tokens[pos]));
        }
        pos++;
    }
    
    try {
        db.getTable(tableName).insert(values);
        cout << "Inserted 1 row\n";
    } catch (const exception& e) {
        cout << "Error: " << e.what() << "\n";
    }
}

// Обработка DELETE запроса
void processDelete(const vector<string>& tokens, Database& db) {
    if (tokens.size() < 3 || tokens[1] != "FROM") {
        cout << "Error: Invalid DELETE syntax\n";
        return;
    }
    string tableName = tokens[2];
    if (!db.hasTable(tableName)) {
        cout << "Error: Table " << tableName << " not found\n";
        return;
    }
    
    vector<string> whereTokens;
    if (tokens.size() > 3 && tokens[3] == "WHERE") {
        for (size_t i = 4; i < tokens.size(); ++i) {
            whereTokens.push_back(tokens[i]);
        }
    }
    
    try {
        Table& table = db.getTable(tableName);
        auto cols = table.getColumns();
        string pk = table.getPkColumnName();
        
        table.deleteRows([&](const vector<string>& row, const vector<string>& colNames) {
            if (whereTokens.empty()) return true;
            
            map<string, string> rowMap;
            for (size_t i = 0; i < colNames.size(); ++i) {
                if (i < row.size()) {
                    if (colNames[i] == pk) {
                        rowMap[pk] = row[i];
                    } else {
                        rowMap[tableName + "." + colNames[i]] = row[i];
                    }
                }
            }
            
            size_t p = 0;
            return evaluateExpression(whereTokens, p, rowMap);
        });
        cout << "Deleted rows\n";
    } catch (const exception& e) {
        cout << "Error: " << e.what() << "\n";
    }
}

int main() {
    try {
        auto schema = Schema::loadFromFile("schema.json");
        Database db(schema);
        cout << "Relational DB started. Schema: " << schema.name << "\n";
        
        string line;
        while (true) {
            cout << "> ";
            if (!getline(cin, line)) break;
            if (line == "exit" || line == "quit") break;
            if (line.empty()) continue;
            
            auto tokens = tokenize(line);
            if (tokens.empty()) continue;
            
            string cmd = tokens[0];
            transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
            
            if (cmd == "SELECT") {
                processSelect(tokens, db);
            } else if (cmd == "INSERT") {
                processInsert(tokens, db);
            } else if (cmd == "DELETE") {
                processDelete(tokens, db);
            } else {
                cout << "Unknown command: " << cmd << "\n";
            }
        }
    } catch (const exception& e) {
        cerr << "Fatal Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
