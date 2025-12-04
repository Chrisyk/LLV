#ifndef SCA_H
#define SCA_H
#include <vector>
#include <string>
#include <memory>
#include <deque>

namespace ConcVLL {

class Transaction;
using txn_ptr = std::shared_ptr<Transaction>;

class SCA {
public:

    static txn_ptr analyze(std::deque<txn_ptr>& queue);
};

}

#endif
