// adt/Array.hpp
#pragma once

#include <nlohmann/json.hpp>
#include <stdexcept>
#include <iostream>
#include <functional>
#include <string>
#include <optional>
#include <fstream>
#include <filesystem>
#include <random>
#include <chrono>
#include <algorithm>
#include <sstream>

namespace adt {
using namespace std;
using json = nlohmann::json;

class Array {
private:
    json* data = nullptr; // Указатель на динамически выделенный массив json-объектов
    size_t _size = 0;            // Текущее количество элементов
    size_t _capacity = 0;        // Общая ёмкость выделенной памяти

    // Увеличиваем ёмкость массива
    void grow() {
        // Если массив пуст — начинаем с ёмкости 1, иначе - удваиваем текущую
        size_t new_cap = (_capacity == 0) ? 1 : _capacity * 2;

        // Выделяем новый блок памяти большего размера
        json* new_data = new json[new_cap];

        // Перемещаем существующие элементы в новый блок
        for (size_t i = 0; i < _size; ++i) {
            new_data[i] = move(data[i]);
        }

        // Освобождаем старую память
        delete[] data;

        // Обновляем указатель и ёмкость
        data = new_data;
        _capacity = new_cap;
    }

public:
    // Создаёт пустой массив (конструктор по умолчанию)
    Array() = default;

    // Освобождает всю выделенную память (деструктор)
    ~Array() {
        delete[] data;
    }

    // Перемещающий конструктор
    Array(Array&& other) noexcept
        : data(other.data), _size(other._size), _capacity(other._capacity) {
        other.data = nullptr;
        other._size = 0;
        other._capacity = 0;
    }

    // Перемещающий оператор присваивания
    Array& operator=(Array&& other) noexcept {
        if (this != &other) {
            delete[] data;
            data = other.data;
            _size = other._size;
            _capacity = other._capacity;
            other.data = nullptr;
            other._size = 0;
            other._capacity = 0;
        }
        return *this;
    }

    // Копирующий конструктор
    Array(const Array& other) : _size(other._size), _capacity(other._capacity) {
        if (_capacity > 0) {
            data = new json[_capacity];
            for (size_t i = 0; i < _size; ++i) {
                data[i] = other.data[i];
            }
        } else {
            data = nullptr;
        }
    }

    // Копирующий оператор присваивания
    Array& operator=(const Array& other) {
        if (this != &other) {
            delete[] data;
            _size = other._size;
            _capacity = other._capacity;
            if (_capacity > 0) {
                data = new json[_capacity];
                for (size_t i = 0; i < _size; ++i) {
                    data[i] = other.data[i];
                }
            } else {
                data = nullptr;
            }
        }
        return *this;
    }

    // Добавляет элемент в конец массива
    void append(const json& value) {
        // Если места нет — расширяем массив
        if (_size >= _capacity) grow();
        // Записываем значение и увеличиваем счётчик
        data[_size++] = value;
    }

    // Добавляет элемент в конец массива
    void append(json&& value) {
        if (_size >= _capacity) grow();
        data[_size++] = move(value);
    }

    // Вставляет элемент по индексу (сдвигая остальные вправо)
    void insert(size_t index, const json& value) {
        // Проверка корректности индекса
        if (index > _size) throw out_of_range("insert: index out of range");

        // При необходимости расширяем массив
        if (_size >= _capacity) grow();

        // Сдвигаем элементы с позиции index вправо на 1
        for (size_t i = _size; i > index; --i) {
            data[i] = move(data[i - 1]);
        }

        // Вставляем новое значение
        data[index] = value;
        _size++;
    }

    // Вставляет элемент по индексу (сдвигая остальные вправо)
    void insert(size_t index, json&& value) {
        if (index > _size) throw out_of_range("insert: index out of range");
        if (_size >= _capacity) grow();
        for (size_t i = _size; i > index; --i) {
            data[i] = move(data[i - 1]);
        }
        data[index] = move(value);
        _size++;
    }

    // Константная версия доступа по индексу с проверкой границ
    const json& at(size_t index) const {
        if (index >= _size) throw out_of_range("at: index out of range");
        return data[index];
    }

    // Неконстантная версия доступа по индексу с проверкой границ
    json& at(size_t index) {
        if (index >= _size) throw out_of_range("at: index out of range");
        return data[index];
    }

    // Удаляет элемент по индексу (сдвигая последующие влево)
    void erase(size_t index) {
        if (index >= _size) throw out_of_range("erase: index out of range");

        // Сдвигаем все элементы после index на одну позицию влево
        for (size_t i = index; i < _size - 1; ++i) {
            data[i] = move(data[i + 1]);
        }

        // Уменьшает логический размер
        _size--;
    }

    // Возвращает текущее количество элементов
    size_t size() const {
        return _size;
    }

    // Проверяет наличие значения в массиве (линейный поиск)
    bool contains(const json& value) const {
        for (size_t i = 0; i < _size; ++i) {
            if (data[i] == value) return true;
        }
        return false;
    }

    // Сериализует массив в JSON-массив
    json serialize() const {
        json j = json::array();
        for (size_t i = 0; i < _size; ++i) {
            j.push_back(data[i]);
        }
        return j;
    }

    // Десериализует массив из JSON
    void deserialize(const json& j) {
        // Освобождает старую память
        delete[] data;

        // Устанавливает новый размер
        _size = j.size();
        _capacity = _size;

        if (_capacity == 0) {
            data = nullptr;
        } else {
            // Выделяет память под точное количество элементов
            data = new json[_capacity];
            for (size_t i = 0; i < _size; ++i) {
                data[i] = j[i];
            }
        }
    }

    // Печатает содержимое массива
    void print() const {
        for (size_t i = 0; i < _size; ++i) {
            cout << "[" << i << "] " << data[i].dump(2) << "\n";
        }
    }

    // Сортировка (метод добавлен в текущей практике)
    void sort(function<bool(const json&, const json&)> comp) {
        if (_size <= 1) return;

        // Реализуем сортировку вставками
        for (size_t i = 1; i < _size; ++i) {
            json key = move(data[i]);
            size_t j = i;

            // Сдвигаем элементы, которые больше key, вправо
            while (j > 0 && comp(key, data[j - 1])) {
                data[j] = move(data[j - 1]);
                --j;
            }

            // Вставляем key на правильную позицию
            data[j] = move(key);
        }
    } 
};

}
