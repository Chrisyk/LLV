#include "sca.h"
#include "../transaction/transaction.h"
#include <functional>

namespace ConcVLL {

constexpr size_t SCA_BITSET_SIZE = 819200;

txn_ptr SCA::analyze(std::deque<txn_ptr>& queue) {
    
    std::vector<bool> Dx(SCA_BITSET_SIZE, false);  
    std::vector<bool> Ds(SCA_BITSET_SIZE, false);  
    std::hash<std::string> hasher;

    for (const auto& T : queue) {
        
        if (!T->hashes_cached) {
            T->hashedReadSet.clear();
            T->hashedReadSet.reserve(T->ReadSet.size());
            for (const auto& key : T->ReadSet) {
                T->hashedReadSet.push_back(hasher(key) % SCA_BITSET_SIZE);
            }
            T->hashedWriteSet.clear();
            T->hashedWriteSet.reserve(T->WriteSet.size());
            for (const auto& key : T->WriteSet) {
                T->hashedWriteSet.push_back(hasher(key) % SCA_BITSET_SIZE);
            }
            T->hashes_cached = true;
        }

        if (T->type == Transaction::Type::Blocked) {
            bool success = true;
            
            for (const auto& hash_val : T->hashedReadSet) {
                if (Dx[hash_val]) {
                    success = false;
                    break;
                }
            }

            if (success) {
                for (const auto& hash_val : T->hashedWriteSet) {
                    if (Dx[hash_val] || Ds[hash_val]) {
                        success = false;
                        break;
                    }
                }
            }

            if (success) {
                
                
                return T;
            }
        }
        
        for (const auto& hash_val : T->hashedReadSet) {
            Ds[hash_val] = true;
        }
        for (const auto& hash_val : T->hashedWriteSet) {
            Dx[hash_val] = true;
        }
    }

    return nullptr;
}

}
