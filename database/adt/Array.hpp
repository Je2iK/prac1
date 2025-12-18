// adt/Array.hpp
#pragma once

#include <functional>

using namespace std;

template<typename T>
class Array {
private:
    T* data = nullptr;
    size_t size = 0;
    size_t capacity = 0;

    void resize() {
        size_t newCap = (capacity == 0) ? 1 : capacity * 2;
        T* newData = new T[newCap];

        for (size_t i = 0; i < size; ++i) {
            newData[i] = std::move(data[i]);
        }

        delete[] data;
        data = newData;
        capacity = newCap;
    }

public:
    Array() = default;

    ~Array() {
        delete[] data;
    }

    Array(Array&& other) noexcept
        : data(other.data), size(other.size), capacity(other.capacity) {
        other.data = nullptr;
        other.size = 0;
        other.capacity = 0;
    }

    Array& operator=(Array&& other) noexcept {
        if (this != &other) {
            delete[] data;
            data = other.data;
            size = other.size;
            capacity = other.capacity;
            other.data = nullptr;
            other.size = 0;
            other.capacity = 0;
        }
        return *this;
    }

    Array(const Array& other) : size(other.size), capacity(other.capacity) {
        if (capacity > 0) {
            data = new T[capacity];
            for (size_t i = 0; i < size; ++i) {
                data[i] = other.data[i];
            }
        } else {
            data = nullptr;
        }
    }

    Array& operator=(const Array& other) {
        if (this != &other) {
            delete[] data;
            size = other.size;
            capacity = other.capacity;
            if (capacity > 0) {
                data = new T[capacity];
                for (size_t i = 0; i < size; ++i) {
                    data[i] = other.data[i];
                }
            } else {
                data = nullptr;
            }
        }
        return *this;
    }

    void append(const T& value) {
        if (size >= capacity) resize();
        data[size++] = value;
    }

    void append(T&& value) {
        if (size >= capacity) resize();
        data[size++] = std::move(value);
    }

    const T& at(size_t index) const {
        if (index >= size) throw std::out_of_range("at: index out of range");
        return data[index];
    }

    T& at(size_t index) {
        if (index >= size) throw std::out_of_range("at: index out of range");
        return data[index];
    }

    size_t getSize() const {
        return size;
    }

    bool empty() const {
        return size == 0;
    }

    void sort(std::function<bool(const T&, const T&)> comp) {
        if (size <= 1) return;

        for (size_t i = 1; i < size; ++i) {
            T key = std::move(data[i]);
            size_t j = i;

            while (j > 0 && comp(key, data[j - 1])) {
                data[j] = std::move(data[j - 1]);
                --j;
            }

            data[j] = std::move(key);
        }
    }
};

