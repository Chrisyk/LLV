#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace ConcVLL {

enum class TxnStatus : uint8_t {
    Active = 0,
    Committed,
    Aborted
};

struct Transaction {
    using id_t = uint64_t;

    id_t id;
    TxnStatus status;

    Transaction() : id(0), status(TxnStatus::Active) {}

    explicit Transaction(id_t i) : id(i), status(TxnStatus::Active) {}

    std::vector<std::string> ReadSet;
    std::vector<std::string> WriteSet;

    enum class Type : uint8_t { Free = 0, Blocked };

    Type type = Type::Blocked;

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;

    Transaction(Transaction&&) = default;
    Transaction& operator=(Transaction&&) = default;

    bool isActive() const noexcept { return status == TxnStatus::Active; }
    bool isCommitted() const noexcept { return status == TxnStatus::Committed; }
    bool isAborted() const noexcept { return status == TxnStatus::Aborted; }
};

using txn_ptr = std::shared_ptr<Transaction>;

}

#endif
