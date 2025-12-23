#include "Database.hpp"
#include <iostream>
#include <string>
#include <sstream>
#include <algorithm>
#include <functional>
#include <filesystem>
#include <fstream>
#include <signal.h>
#include <thread>
#include <mutex>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include "../adt/Array.hpp"
#include "../adt/ChainingHashTable.hpp"

using namespace std;

string g_lockFile;
mutex g_dbMutex;

void signalHandler(int) {
    if (!g_lockFile.empty() && filesystem::exists(g_lockFile)) {
        filesystem::remove(g_lockFile);
    }
    _exit(0);
}

Array<string> tokenize(const string& query) {
    Array<string> tokens;
    string current;
    bool inQuote = false;
    for (size_t i = 0; i < query.length(); ++i) {
        char c = query[i];
        if (inQuote) {
            if (c == '\'') {
                inQuote = false;
                current += c;
                tokens.append(current);
                current = "";
            } else {
                current += c;
            }
        } else {
            if (isspace(c)) {
                if (!current.empty()) {
                    tokens.append(current);
                    current = "";
                }
            } else if (c == ',' || c == '=' || c == '(' || c == ')') {
                if (!current.empty()) {
                    tokens.append(current);
                    current = "";
                }
                tokens.append(string(1, c));
            } else if (c == '\'') {
                if (!current.empty()) {
                     tokens.append(current);
                     current = "";
                }
                inQuote = true;
                current += c;
            } else {
                current += c;
            }
        }
    }
    if (!current.empty()) tokens.append(current);
    return tokens;
}

string stripQuotes(const string& s) {
    if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

bool evaluateExpression(const Array<string>& tokens, size_t& pos, const ChainingHashTable<string, string>& rowData);

string getOperandValue(const string& operand, const ChainingHashTable<string, string>& rowData) {
    if (operand.size() >= 2 && operand.front() == '\'' && operand.back() == '\'') {
        return stripQuotes(operand);
    }
    if (rowData.find(operand)) {
        return rowData.at(operand);
    }
    return operand; 
}

bool evaluateCondition(const Array<string>& tokens, size_t& pos, const ChainingHashTable<string, string>& rowData) {
    if (pos >= tokens.getSize()) return false;
    string lhs = tokens.at(pos++);
    if (pos >= tokens.getSize() || tokens.at(pos) != "=") return false;
    pos++;
    if (pos >= tokens.getSize()) return false;
    string rhs = tokens.at(pos++);
    
    string lVal = getOperandValue(lhs, rowData);
    string rVal = getOperandValue(rhs, rowData);
    return lVal == rVal;
}

bool evaluateFactor(const Array<string>& tokens, size_t& pos, const ChainingHashTable<string, string>& rowData) {
    if (pos >= tokens.getSize()) return false;
    if (tokens.at(pos) == "(") {
        pos++;
        bool result = evaluateExpression(tokens, pos, rowData);
        if (pos < tokens.getSize() && tokens.at(pos) == ")") pos++;
        return result;
    }
    return evaluateCondition(tokens, pos, rowData);
}

bool evaluateTerm(const Array<string>& tokens, size_t& pos, const ChainingHashTable<string, string>& rowData) {
    bool left = evaluateFactor(tokens, pos, rowData);
    while (pos < tokens.getSize() && tokens.at(pos) == "AND") {
        pos++;
        bool right = evaluateFactor(tokens, pos, rowData);
        left = left && right;
    }
    return left;
}

bool evaluateExpression(const Array<string>& tokens, size_t& pos, const ChainingHashTable<string, string>& rowData) {
    bool left = evaluateTerm(tokens, pos, rowData);
    while (pos < tokens.getSize() && tokens.at(pos) == "OR") {
        pos++;
        bool right = evaluateTerm(tokens, pos, rowData);
        left = left || right;
    }
    return left;
}

string processSelect(const Array<string>& tokens, Database& db) {
    stringstream result;
    
    size_t pos = 1;
    Array<string> projection;
    while (pos < tokens.getSize() && tokens.at(pos) != "FROM") {
        if (tokens.at(pos) != ",") {
            projection.append(tokens.at(pos));
        }
        pos++;
    }
    
    if (pos >= tokens.getSize() || tokens.at(pos) != "FROM") {
        return "Error: Expected FROM\n";
    }
    pos++;
    
    Array<string> tableNames;
    while (pos < tokens.getSize() && tokens.at(pos) != "WHERE") {
        if (tokens.at(pos) != ",") {
            tableNames.append(tokens.at(pos));
        }
        pos++;
    }
    
    Array<string> whereTokens;
    if (pos < tokens.getSize() && tokens.at(pos) == "WHERE") {
        pos++;
        while (pos < tokens.getSize()) {
            whereTokens.append(tokens.at(pos++));
        }
    }
    
    struct TableInfo {
        string name;
        Array<string> columns;
        string pkName;
        Array<filesystem::path> files;
    };
    
    Array<TableInfo> tables;
    for (size_t i = 0; i < tableNames.getSize(); ++i) {
        const string& tName = tableNames.at(i);
        if (!db.hasTable(tName)) {
            return "Error: Table " + tName + " not found\n";
        }
        Table& table = db.getTable(tName);
        TableInfo tInfo;
        tInfo.name = tName;
        tInfo.columns = table.getColumns();
        tInfo.pkName = table.getPkColumnName();
        
        filesystem::path basePath = filesystem::path(db.getSchemaName()) / tName;
        if (filesystem::exists(basePath)) {
            for (const auto& entry : filesystem::directory_iterator(basePath)) {
                if (entry.path().extension() == ".csv") {
                    tInfo.files.append(entry.path());
                }
            }
            tInfo.files.sort([](const filesystem::path& a, const filesystem::path& b) {
                try {
                    return stoi(a.stem().string()) < stoi(b.stem().string());
                } catch (...) {
                    return a.stem().string() < b.stem().string();
                }
            });
        }
        tables.append(tInfo);
    }
    
    auto parseCsvLine = [](const string& line) -> Array<string> {
        Array<string> result;
        stringstream ss(line);
        string cell;
        while (getline(ss, cell, ',')) {
            result.append(cell);
        }
        return result;
    };
    
    auto createRowMap = [](const Array<string>& row, const TableInfo& tInfo) -> ChainingHashTable<string, string> {
        ChainingHashTable<string, string> rowMap;
        if (row.getSize() > 0) rowMap.insert(tInfo.pkName, row.at(0));
        for (size_t i = 0; i < tInfo.columns.getSize(); ++i) {
            if (i + 1 < row.getSize()) {
                rowMap.insert(tInfo.name + "." + tInfo.columns.at(i), row.at(i + 1));
                rowMap.insert(tInfo.columns.at(i), row.at(i + 1));
            }
        }
        return rowMap;
    };
    
    function<void(size_t, ChainingHashTable<string, string>)> processCartesian;
    processCartesian = [&](size_t tableIdx, ChainingHashTable<string, string> currentRow) {
        if (tableIdx >= tables.getSize()) {
            bool match = true;
            if (!whereTokens.empty()) {
                size_t p = 0;
                match = evaluateExpression(whereTokens, p, currentRow);
            }
            
            if (match) {
                for (size_t i = 0; i < projection.getSize(); ++i) {
                    if (i > 0) result << ",";
                    if (currentRow.find(projection.at(i))) {
                        result << currentRow.at(projection.at(i));
                    } else {
                        result << "NULL";
                    }
                }
                result << "\n";
            }
            return;
        }
        
        const TableInfo& tInfo = tables.at(tableIdx);
        for (size_t i = 0; i < tInfo.files.getSize(); ++i) {
            ifstream f(tInfo.files.at(i));
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
                
                ChainingHashTable<string, string> combined = currentRow;
                auto keys = rowMap.getAllKeys();
                for (size_t k = 0; k < keys.getSize(); ++k) {
                    combined.insert(keys.at(k), rowMap.at(keys.at(k)));
                }
                
                processCartesian(tableIdx + 1, combined);
            }
        }
    };
    
    processCartesian(0, ChainingHashTable<string, string>());
    return result.str();
}

string processInsert(const Array<string>& tokens, Database& db) {
    if (tokens.getSize() < 6 || tokens.at(1) != "INTO" || tokens.at(3) != "VALUES") {
        return "Error: Invalid INSERT syntax\n";
    }
    string tableName = tokens.at(2);
    if (!db.hasTable(tableName)) {
        return "Error: Table " + tableName + " not found\n";
    }
    
    Array<string> values;
    size_t pos = 5;
    while (pos < tokens.getSize() && tokens.at(pos) != ")") {
        if (tokens.at(pos) != ",") {
            values.append(stripQuotes(tokens.at(pos)));
        }
        pos++;
    }
    
    try {
        db.getTable(tableName).insert(values);
        return "Inserted 1 row\n";
    } catch (const exception& e) {
        return string("Error: ") + e.what() + "\n";
    }
}

string processDelete(const Array<string>& tokens, Database& db) {
    if (tokens.getSize() < 3 || tokens.at(1) != "FROM") {
        return "Error: Invalid DELETE syntax\n";
    }
    string tableName = tokens.at(2);
    if (!db.hasTable(tableName)) {
        return "Error: Table " + tableName + " not found\n";
    }
    
    Array<string> whereTokens;
    if (tokens.getSize() > 3 && tokens.at(3) == "WHERE") {
        for (size_t i = 4; i < tokens.getSize(); ++i) {
            whereTokens.append(tokens.at(i));
        }
    }
    
    try {
        Table& table = db.getTable(tableName);
        
        table.deleteRows([&](const Array<string>& row, const Array<string>& colNames) {
            if (whereTokens.empty()) return true;
            
            ChainingHashTable<string, string> rowMap;
            for (size_t i = 0; i < colNames.getSize(); ++i) {
                if (i < row.getSize()) {
                    rowMap.insert(colNames.at(i), row.at(i));
                }
            }
            
            size_t p = 0;
            return evaluateExpression(whereTokens, p, rowMap);
        });
        return "Deleted rows\n";
    } catch (const exception& e) {
        return string("Error: ") + e.what() + "\n";
    }
}

string executeQuery(const string& query, Database& db) {
    if (query.empty()) return "";
    
    auto tokens = tokenize(query);
    if (tokens.empty()) return "";
    
    string cmd = tokens.at(0);
    transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    
    if (cmd == "SELECT") {
        return processSelect(tokens, db);
    }
    
    lock_guard<mutex> lock(g_dbMutex);
    
     if (cmd == "INSERT") {
        return processInsert(tokens, db);
    } else if (cmd == "DELETE") {
        return processDelete(tokens, db);
    } else {
        return "Unknown command: " + cmd + "\n";
    }
}

void handleClient(int clientSocket, Database& db) {
    char buffer[4096];
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytesRead = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
        
        if (bytesRead <= 0) break;
        
        string query(buffer);
        query.erase(query.find_last_not_of(" \n\r\t") + 1);
        
        if (query == "quit") {
            send(clientSocket, "пока!\n", 9, 0);
            break;
        }
        
        string result = executeQuery(query, db);
        send(clientSocket, result.c_str(), result.length(), 0);
    }
    
    close(clientSocket);
}

int main() {
    try {
        auto schema = Schema::loadFromFile("schema.json");
        Database db(schema);
        g_lockFile = filesystem::path(schema.name) / ".db_lock";
        
        signal(SIGINT, signalHandler);
        signal(SIGTERM, signalHandler);
        
        cout << "Schema: " << schema.name << endl;
        cout << "Available tables: ";
        auto tableNames = db.getTableNames();
        for (size_t i = 0; i < tableNames.getSize(); ++i) {
            cout << tableNames.at(i);
            if (i < tableNames.getSize() - 1) cout << ", ";
        }
        cout << endl;
        
        int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (serverSocket < 0) {
            cerr << "Ошибка создания сокета\n";
            return 1;
        }
        
        int opt = 1;
        setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        
        sockaddr_in serverAddr{};
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_addr.s_addr = INADDR_ANY;
        serverAddr.sin_port = htons(7432);
        
        if (bind(serverSocket, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
            cerr << "не удалось подключиться к 7432 порту\n";
            close(serverSocket);
            return 1;
        }
        
        if (listen(serverSocket, 10) < 0) {
            cerr << "не прослушивается\n";
            close(serverSocket);
            return 1;
        }
        
        cout << "Сервер запущен на 7432 порту\n";
        
        while (true) {
            sockaddr_in clientAddr{};
            socklen_t clientLen = sizeof(clientAddr);
            int clientSocket = accept(serverSocket, (sockaddr*)&clientAddr, &clientLen);
            
            if (clientSocket < 0) continue;
            
            thread(handleClient, clientSocket, ref(db)).detach();
        }
        
        close(serverSocket);
        g_lockFile = "";
    } catch (const exception& e) {
        cerr << "Ошибка: " << e.what() << "\n";
        g_lockFile = "";
        return 1;
    }
    return 0;
}
