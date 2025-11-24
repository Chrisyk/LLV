#ifndef VLL_H
#define VLL_H

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include "../transaction/transaction.h"
#include "../core/vll_stman.h"
#include <functional>

namespace ConcVLL {

class TxnQueue {
public:
	TxnQueue();

	void BeginTransaction(const txn_ptr& T, ::storageManager& store);

	void FinishTransaction(const txn_ptr& T, ::storageManager& store);

	txn_ptr beginTransaction();

	void finishTransaction(const txn_ptr& txn);

	std::size_t activeCount() const;

	void CancelAll(::storageManager& store);

	void VLLMainLoop(::storageManager& store,
					 std::function<void(txn_ptr)> execute,
					 std::function<txn_ptr()> getNewTxnRequest,
					 std::function<bool()> shouldStop,
					 std::size_t maxQueueSize = 1024);

private:
	mutable std::mutex mtx_;
	std::deque<txn_ptr> queue_;
	std::atomic<Transaction::id_t> nextId_{1};
};

}

#endif
