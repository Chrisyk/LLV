#include "vll_stman.h"
#include <iostream>

storageManager::storageManager() { }

void storageManager::insert(const std::string& key, const std::string value){
    data.erase(key);
    data.try_emplace(key, value);
}

tuple* storageManager::get(const std::string& key){
    auto it = data.find(key);
    return it != data.end() ? &it->second : nullptr;
}

void storageManager::remove(const std::string& key){
    data.erase(key);
}

void storageManager::rangeQuery(const std::string& startKey, const std::string& endKey){
    for (auto &p : data) {
        const auto &k = p.first;
        if (k >= startKey && k <= endKey) {
            std::cout << k << ": " << p.second.value << '\n';
        }
    }
}

