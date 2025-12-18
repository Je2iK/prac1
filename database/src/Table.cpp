#include "Table.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <thread>
#include <chrono>
#include <iostream>


Table::Table(const TableConfig & config) : config(config) {
    filesystem::create_directories(config.basePath);
    pkColumnName = config.name + "_pk";
    pkSequenceFile = config.basePath / (config.name + "_pk_sequence");
    lockFile = config.basePath / (config.name + "_lock");
    
    if (!filesystem::exists(pkSequenceFile)) {
        ofstream f(pkSequenceFile);
        f << 0;
    }
}

void Table::lock() {
    int retries = 0;
    while (filesystem::exists(lockFile)) {
        this_thread::sleep_for(chrono::milliseconds(10));
        retries++;
        if (retries > 1000) {
             throw runtime_error("Table lock timeout");
        }
    }
    ofstream f(lockFile);
    f << "locked";
}

void Table::unlock() {
    filesystem::remove(lockFile);
}

size_t Table::getNextId() {
    ifstream ifile(pkSequenceFile);
    size_t id = 0;
    if (ifile.is_open()) {
        ifile >> id;
    }
    id++;
    ifile.close();
    
    ofstream ofile(pkSequenceFile);
    ofile << id;
    return id;
}

const Array<string>& Table::getColumns() const {
    return config.columns;
}

string Table::getPkColumnName() const {
    return pkColumnName;
}

void Table::insert(const Array<string>& values) {
    lock();
    try {
        size_t id = getNextId();
        Array<string> fullRow;
        fullRow.append(to_string(id));
        for (size_t i = 0; i < values.getSize(); ++i) {
            fullRow.append(values.at(i));
        }

        if (fullRow.getSize() != config.columns.getSize() + 1) {
            throw runtime_error("Column count mismatch. Expected " + to_string(config.columns.getSize()) + " values, got " + to_string(values.getSize()));
        }
        
        filesystem::path file = getCurrentDataFilePath();
        bool newFile = !filesystem::exists(file);
        
        ofstream f(file, ios::app);
        if (newFile) {
            f << pkColumnName;
            for (size_t i = 0; i < config.columns.getSize(); ++i) {
                f << "," << config.columns.at(i);
            }
            f << "\n";
        }
        
        for (size_t i = 0; i < fullRow.getSize(); ++i) {
            if (i > 0) f << ",";
            f << fullRow.at(i);
        }
        f << "\n";
        
    } catch (...) {
        unlock();
        throw;
    }
    unlock();
}

Array<Array<string>> Table::scan() {
    Array<Array<string>> allRows;
    auto files = getDataFiles();
    for (size_t i = 0; i < files.getSize(); ++i) {
        ifstream f(files.at(i));
        string line;
        bool header = true;
        while (getline(f, line)) {
            if (line.empty()) continue;
            if (header) {
                header = false;
                continue;
            }
            
            Array<string> row;
            stringstream ss(line);
            string cell;
            while (getline(ss, cell, ',')) {
                row.append(cell);
            }
            allRows.append(row);
        }
    }
    return allRows;
}

void Table::deleteRows(const function<bool(const Array<string>&, const Array<string>&)>& predicate) {
    lock();
    try {
        auto files = getDataFiles();
        Array<string> allColumns;
        allColumns.append(pkColumnName);
        for (size_t i = 0; i < config.columns.getSize(); ++i) {
            allColumns.append(config.columns.at(i));
        }

        for (size_t i = 0; i < files.getSize(); ++i) {
            ifstream f(files.at(i));
            string line;
            Array<string> linesToKeep;
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
                
                Array<string> row;
                stringstream ss(line);
                string cell;
                while (getline(ss, cell, ',')) {
                    row.append(cell);
                }
                
                if (!predicate(row, allColumns)) {
                    linesToKeep.append(line);
                } else {
                    modified = true;
                }
            }
            f.close();
            
            if (modified) {
                ofstream of(files.at(i));
                of << headerLine << "\n";
                for (size_t j = 0; j < linesToKeep.getSize(); ++j) {
                    of << linesToKeep.at(j) << "\n";
                }
            }
        }
    } catch (...) {
        unlock();
        throw;
    }
    unlock();
}

Array<filesystem::path> Table::getDataFiles() const {
    Array<filesystem::path> files;
    if (!filesystem::exists(config.basePath)) return files;

    for (const auto& entry : filesystem::directory_iterator(config.basePath)) {
        if (entry.path().extension() == ".csv") {
            files.append(entry.path());
        }
    }
    files.sort([](const filesystem::path& a, const filesystem::path& b) {
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

size_t Table::getCurrentFileRowCount() const {
    auto files = getDataFiles();
    if (files.empty()) return 0;
    
    ifstream f(files.at(files.getSize() - 1));
    size_t lines = 0;
    string line;
    while (getline(f, line)) {
        if (!line.empty()) lines++;
    }
    return lines > 0 ? lines - 1 : 0;
}

filesystem::path Table::getCurrentDataFilePath() const {
    auto files = getDataFiles();
    if (files.empty()) return config.basePath / "1.csv";
    
    if (getCurrentFileRowCount() >= config.tuplesLimit) {
        string stem = files.at(files.getSize() - 1).stem().string();
        try {
            int nextNum = stoi(stem) + 1;
            return config.basePath / (to_string(nextNum) + ".csv");
        } catch (...) {
            return config.basePath / "1_new.csv"; 
        }
    }
    return files.at(files.getSize() - 1);
}


