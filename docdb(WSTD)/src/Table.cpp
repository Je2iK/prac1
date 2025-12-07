#include "Table.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <thread>
#include <chrono>
#include <iostream>

namespace docdb {

// Конструктор таблицы - инициализация путей и файлов
Table::Table(const TableConfig & config) : config_(config) {
    filesystem::create_directories(config_.basePath);
    pkColumnName_ = config_.name + "_pk";
    pkSequenceFile_ = config_.basePath / (config_.name + "_pk_sequence");
    lockFile_ = config_.basePath / (config_.name + "_lock");
    
    if (!filesystem::exists(pkSequenceFile_)) {
        ofstream f(pkSequenceFile_);
        f << 0;
    }
}

// Блокировка таблицы для конкурентного доступа
void Table::lock() {
    int retries = 0;
    while (filesystem::exists(lockFile_)) {
        this_thread::sleep_for(chrono::milliseconds(10));
        retries++;
        if (retries > 1000) {
             throw runtime_error("Table lock timeout");
        }
    }
    ofstream f(lockFile_);
    f << "locked";
}

void Table::unlock() {
    filesystem::remove(lockFile_);
}

// Получение и инкремент следующего ID для первичного ключа
size_t Table::getNextId() {
    ifstream ifile(pkSequenceFile_);
    size_t id = 0;
    if (ifile.is_open()) {
        ifile >> id;
    }
    id++;
    ifile.close();
    
    ofstream ofile(pkSequenceFile_);
    ofile << id;
    return id;
}

const vector<string>& Table::getColumns() const {
    return config_.columns;
}

string Table::getPkColumnName() const {
    return pkColumnName_;
}

// Вставка новой строки в таблицу
void Table::insert(const vector<string>& values) {
    lock();
    try {
        size_t id = getNextId();
        vector<string> fullRow;
        fullRow.push_back(to_string(id));
        fullRow.insert(fullRow.end(), values.begin(), values.end());

        if (fullRow.size() != config_.columns.size() + 1) {
            throw runtime_error("Column count mismatch. Expected " + to_string(config_.columns.size()) + " values, got " + to_string(values.size()));
        }
        
        filesystem::path file = getCurrentDataFilePath();
        bool newFile = !filesystem::exists(file);
        
        ofstream f(file, ios::app);
        if (newFile) {
            f << pkColumnName_;
            for (const auto& col : config_.columns) {
                f << "," << col;
            }
            f << "\n";
        }
        
        for (size_t i = 0; i < fullRow.size(); ++i) {
            if (i > 0) f << ",";
            f << fullRow[i];
        }
        f << "\n";
        
    } catch (...) {
        unlock();
        throw;
    }
    unlock();
}

// Полное сканирование всех строк таблицы
vector<vector<string>> Table::scan() {
    vector<vector<string>> allRows;
    auto files = getDataFiles();
    for (const auto& file : files) {
        ifstream f(file);
        string line;
        bool header = true;
        while (getline(f, line)) {
            if (line.empty()) continue;
            if (header) {
                header = false;
                continue;
            }
            
            vector<string> row;
            stringstream ss(line);
            string cell;
            while (getline(ss, cell, ',')) {
                row.push_back(cell);
            }
            allRows.push_back(row);
        }
    }
    return allRows;
}

// Удаление строк по предикату
void Table::deleteRows(const function<bool(const vector<string>&, const vector<string>&)>& predicate) {
    lock();
    try {
        auto files = getDataFiles();
        vector<string> allColumns;
        allColumns.push_back(pkColumnName_);
        allColumns.insert(allColumns.end(), config_.columns.begin(), config_.columns.end());

        for (const auto& file : files) {
            ifstream f(file);
            string line;
            vector<string> linesToKeep;
            bool header = true;
            string headerLine;
            bool modified = false;
            
            while (getline(f, line)) {
                if (line.empty()) continue;
                if (header) {
                    headerLine = line;
                    header = false;
                    continue;
                }
                
                vector<string> row;
                stringstream ss(line);
                string cell;
                while (getline(ss, cell, ',')) {
                    row.push_back(cell);
                }
                
                if (!predicate(row, allColumns)) {
                    linesToKeep.push_back(line);
                } else {
                    modified = true;
                }
            }
            f.close();
            
            if (modified) {
                ofstream of(file);
                of << headerLine << "\n";
                for (const auto& l : linesToKeep) {
                    of << l << "\n";
                }
            }
        }
    } catch (...) {
        unlock();
        throw;
    }
    unlock();
}

// Получение списка CSV файлов таблицы
vector<filesystem::path> Table::getDataFiles() const {
    vector<filesystem::path> files;
    if (!filesystem::exists(config_.basePath)) return files;

    for (const auto& entry : filesystem::directory_iterator(config_.basePath)) {
        if (entry.path().extension() == ".csv") {
            files.push_back(entry.path());
        }
    }
    sort(files.begin(), files.end(), [](const filesystem::path& a, const filesystem::path& b) {
        string sa = a.stem().string();
        string sb = b.stem().string();
        try {
            return stoi(sa) < stoi(sb);
        } catch (...) {
            return sa < sb;
        }
    });
    return files;
}

// Подсчёт строк в текущем файле
size_t Table::getCurrentFileRowCount() const {
    auto files = getDataFiles();
    if (files.empty()) return 0;
    
    ifstream f(files.back());
    size_t lines = 0;
    string line;
    while (getline(f, line)) {
        if (!line.empty()) lines++;
    }
    return lines > 0 ? lines - 1 : 0;
}

// Определение пути для текущего CSV файла (с учётом переполнения)
filesystem::path Table::getCurrentDataFilePath() const {
    auto files = getDataFiles();
    if (files.empty()) return config_.basePath / "1.csv";
    
    if (getCurrentFileRowCount() >= config_.tuplesLimit) {
        string stem = files.back().stem().string();
        try {
            int nextNum = stoi(stem) + 1;
            return config_.basePath / (to_string(nextNum) + ".csv");
        } catch (...) {
            return config_.basePath / "1_new.csv"; 
        }
    }
    return files.back();
}

}
