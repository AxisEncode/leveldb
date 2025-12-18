#ifndef LEVELDB_TRANSACTION_H_
#define LEVELDB_TRANSACTION_H_

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <string>

namespace leveldb {

class Transaction {
 public:
  explicit Transaction(DB* db);
  ~Transaction();

  // Disable copy and assignment
  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

  // Read operations
  Status Get(const ReadOptions& options, const Slice& key, std::string* value);

  // Write operations
  Status Put(const Slice& key, const Slice& value);
  Status Delete(const Slice& key);

  // Transaction control
  Status Commit();
  Status Abort();

  // Transaction state
  enum class TransactionState {
    ACTIVE,
    COMMITTED,
    ABORTED
  };
  TransactionState state() const { return state_; }

 private:
  struct BufferEntry {
    std::string value;
    bool is_tombstone;
    
    BufferEntry() : is_tombstone(false) {}
    explicit BufferEntry(const std::string& v) : value(v), is_tombstone(false) {}
    static BufferEntry Tombstone() {
      BufferEntry entry;
      entry.is_tombstone = true;
      return entry;
    }
  };

  Status CheckActive() const;
  Status ValidateWriteSet();

  static std::mutex commit_mutex_;

  DB* db_;
  const Snapshot* snapshot_;
  TransactionState state_;

  std::unordered_set<std::string> read_set_;  // Track read keys for validation
  std::unordered_map<std::string, BufferEntry> write_buffer_; // Track writes with proper tombstone semantics
  WriteBatch write_batch_;
};

}  // namespace leveldb

#endif  // LEVELDB_TRANSACTION_H_