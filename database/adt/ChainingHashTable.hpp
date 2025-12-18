#pragma once

#include "Array.hpp"

template <typename K, typename V>
class ChainingHashTable {
private:
    struct Node {
        K key;
        V value;
        Node* next;
        
        Node(const K& k, const V& v) : key(k), value(v), next(nullptr) {}
    };
    
    Array<Node*> buckets;
    size_t numElements;
    size_t capacity;
    static constexpr size_t DEFAULT_CAPACITY = 16;
    static constexpr double LOAD_FACTOR_THRESHOLD = 0.75;
    
    size_t hash(const K& key) const {
        return std::hash<K>{}(key) % capacity;
    }
    
    void rehash() {
        size_t oldCapacity = capacity;
        capacity *= 2;
        Array<Node*> oldBuckets = std::move(buckets);
        buckets = Array<Node*>();
        for (size_t i = 0; i < capacity; ++i) {
            buckets.append(nullptr);
        }
        numElements = 0;
        
        for (size_t i = 0; i < oldCapacity; ++i) {
            Node* current = oldBuckets.at(i);
            while (current != nullptr) {
                Node* next = current->next;
                insert(current->key, current->value);
                delete current;
                current = next;
            }
        }
    }
    
public:
    ChainingHashTable() : numElements(0), capacity(DEFAULT_CAPACITY) {
        for (size_t i = 0; i < capacity; ++i) {
            buckets.append(nullptr);
        }
    }
    
    ~ChainingHashTable() {
        for (size_t i = 0; i < capacity; ++i) {
            Node* current = buckets.at(i);
            while (current != nullptr) {
                Node* next = current->next;
                delete current;
                current = next;
            }
        }
    }
    
    ChainingHashTable(const ChainingHashTable& other) 
        : numElements(0), capacity(other.capacity) {
        for (size_t i = 0; i < capacity; ++i) {
            buckets.append(nullptr);
        }
        for (size_t i = 0; i < other.capacity; ++i) {
            Node* current = other.buckets.at(i);
            while (current != nullptr) {
                insert(current->key, current->value);
                current = current->next;
            }
        }
    }
    
    ChainingHashTable& operator=(const ChainingHashTable& other) {
        if (this != &other) {
            // Clear current data
            for (size_t i = 0; i < capacity; ++i) {
                Node* current = buckets.at(i);
                while (current != nullptr) {
                    Node* next = current->next;
                    delete current;
                    current = next;
                }
            }
            
            capacity = other.capacity;
            numElements = 0;
            buckets = Array<Node*>();
            for (size_t i = 0; i < capacity; ++i) {
                buckets.append(nullptr);
            }
            
            for (size_t i = 0; i < other.capacity; ++i) {
                Node* current = other.buckets.at(i);
                while (current != nullptr) {
                    insert(current->key, current->value);
                    current = current->next;
                }
            }
        }
        return *this;
    }
    
    void insert(const K& key, const V& value) {
        size_t index = hash(key);
        
        Node* current = buckets.at(index);
        while (current != nullptr) {
            if (current->key == key) {
                current->value = value; 
                return;
            }
            current = current->next;
        }
        
        Node* newNode = new Node(key, value);
        newNode->next = buckets.at(index);
        buckets.at(index) = newNode;
        numElements++;
        
        if (static_cast<double>(numElements) / capacity > LOAD_FACTOR_THRESHOLD) {
            rehash();
        }
    }
    
    bool find(const K& key) const {
        size_t index = hash(key);
        Node* current = buckets.at(index);
        
        while (current != nullptr) {
            if (current->key == key) {
                return true;
            }
            current = current->next;
        }
        
        return false;
    }
    
    const V& at(const K& key) const {
        size_t index = hash(key);
        Node* current = buckets.at(index);
        
        while (current != nullptr) {
            if (current->key == key) {
                return current->value;
            }
            current = current->next;
        }
        
        throw std::out_of_range("Key not found");
    }
    
    V* getPointer(const K& key) {
        size_t index = hash(key);
        Node* current = buckets.at(index);
        
        while (current != nullptr) {
            if (current->key == key) {
                return &(current->value);
            }
            current = current->next;
        }
        
        return nullptr;
    }
    
    const V* getPointer(const K& key) const {
        size_t index = hash(key);
        Node* current = buckets.at(index);
        
        while (current != nullptr) {
            if (current->key == key) {
                return &(current->value);
            }
            current = current->next;
        }
        
        return nullptr;
    }
    
    void remove(const K& key) {
        size_t index = hash(key);
        Node* current = buckets.at(index);
        Node* prev = nullptr;
        
        while (current != nullptr) {
            if (current->key == key) {
                if (prev == nullptr) {
                    buckets.at(index) = current->next;
                } else {
                    prev->next = current->next;
                }
                delete current;
                numElements--;
                return;
            }
            prev = current;
            current = current->next;
        }
    }
    
    size_t size() const {
        return numElements;
    }
    
    bool empty() const {
        return numElements == 0;
    }
    
    Array<K> getAllKeys() const {
        Array<K> keys;
        for (size_t i = 0; i < capacity; ++i) {
            Node* current = buckets.at(i);
            while (current != nullptr) {
                keys.append(current->key);
                current = current->next;
            }
        }
        return keys;
    }
};
