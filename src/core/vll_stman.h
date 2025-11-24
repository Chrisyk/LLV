#ifndef STORAGE_MANAGER_H
#define STORAGE_MANAGER_H

#include "record.h"
#include <unordered_map>
#include <string>

class storageManager {

  private:
    std::unordered_map<std::string, tuple> data;
  public:
  void insert(const std::string& key, const std::string value);
    tuple* get(const std::string& key);
    void remove(const std::string& key);
    void rangeQuery(const std::string& startKey, const std::string& endKey);
    storageManager();
};

#endif

