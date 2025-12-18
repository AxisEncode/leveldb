// Copyright (c) 2025 The LevelDB Authors. All rights reserved.
//
// Implementation for the C++ snapshot-isolated Transaction API for LevelDB.

#include "db/transaction.h"

namespace leveldb {

std::mutex Transaction::commit_mutex_;

Transaction::Transaction(DB* db)
    : db_(db),
      snapshot_(nullptr),
      state_(TransactionState::ACTIVE) {
  if (db_ == nullptr) {
    state_ = TransactionState::ABORTED;
    return;
  }
  
  // Acquire a snapshot at transaction begin for snapshot isolation
  snapshot_ = db_->GetSnapshot();
  if (snapshot_ == nullptr) {
    state_ = TransactionState::ABORTED;
  }
}

Transaction::~Transaction() {
  if (snapshot_ && db_) {
    db_->ReleaseSnapshot(snapshot_);
  }
}

Status Transaction::CheckActive() const {
  if (state_ == TransactionState::ABORTED) {
    return Status::InvalidArgument("Transaction has been aborted");
  }
  if (state_ == TransactionState::COMMITTED) {
    return Status::InvalidArgument("Transaction has been committed");
  }
  if (db_ == nullptr) {
    return Status::InvalidArgument("Database is null");
  }
  if (snapshot_ == nullptr) {
    return Status::InvalidArgument("Snapshot is null");
  }
  return Status::OK();
}

static Status ValidateSnapshotIsolation(DB* db, const Snapshot* snapshot,
                                       const Slice& key, std::string* value) {
  std::string value_at_snapshot;

  // Read value at our snapshot
  ReadOptions snap_opts;
  snap_opts.snapshot = snapshot;
  Status snap_status = db->Get(snap_opts, key, &value_at_snapshot);

  // Read current value (latest committed state)
  ReadOptions current_opts;
  Status current_status = db->Get(current_opts, key, value);

  // Check for conflicts
  bool existed_at_snapshot = snap_status.ok();
  bool exists_now = current_status.ok();

  if (existed_at_snapshot != exists_now) {
    // Key was added or deleted
    return Status::Corruption(
        "Read-write conflict: key modified by another transaction", key.ToString());
  }

  if (existed_at_snapshot && exists_now && value_at_snapshot != *value) {
    // Key value changed
    return Status::Corruption(
        "Read-write conflict: key modified by another transaction", key.ToString());
  }

  return Status::OK();
}

Status Transaction::Get(const ReadOptions& options, const Slice& key, 
                        std::string* value) {
  Status s = CheckActive();
  if (!s.ok()) {
    return s;
  }

  std::string key_str = key.ToString();
  
  // First check the write buffer for read-your-own-writes
  auto it = write_buffer_.find(key_str);
  if (it != write_buffer_.end()) {
    if (it->second.is_tombstone) {
      // Key was deleted in this transaction
      return Status::NotFound("Key deleted in transaction");
    }
    *value = it->second.value;
    return Status::OK();
  }

  s = ValidateSnapshotIsolation(db_, snapshot_, key, value);
  if (!s.ok()) {
    Abort();
  }
  read_set_.insert(key_str);
  return s;
}

Status Transaction::Put(const Slice& key, const Slice& value) {
  Status s = CheckActive();
  if (!s.ok()) {
    return s;
  }

  std::string key_str = key.ToString();
  std::string value_str = value.ToString();

  // Update write buffer
  write_buffer_[key_str] = BufferEntry(value_str);
  
  // Add to write batch
  write_batch_.Put(key, value);
  
  return Status::OK();
}

Status Transaction::Delete(const Slice& key) {
  Status s = CheckActive();
  if (!s.ok()) {
    return s;
  }

  std::string key_str = key.ToString();

  // Mark as tombstone in write buffer
  write_buffer_[key_str] = BufferEntry::Tombstone();
  
  // Add to write batch
  write_batch_.Delete(key);
  
  return Status::OK();
}

Status Transaction::Commit() {
  Status s = CheckActive();
  if (!s.ok()) {
    return s;
  }

  // If no writes, just mark as committed
  if (write_buffer_.empty()) {
    state_ = TransactionState::COMMITTED;
    return Status::OK();
  }

  // Acquire global commit lock to prevent race conditions during validation
  // This ensures atomicity of validation + commit
  std::lock_guard<std::mutex> lock(commit_mutex_);
  for (const auto& key : read_set_) {
    std::string value;
    Slice key_slice(key);
    s = ValidateSnapshotIsolation(db_, snapshot_, key_slice, &value);
    if (!s.ok()) {
      return s;
    }
  }
  for (const auto& entry : write_buffer_) {
    std::string value;
    const std::string& key_str = entry.first;
    Slice key(key_str);
    Status s = ValidateSnapshotIsolation(db_, snapshot_, key, &value);
    if (!s.ok()) {
      return s;
    }
  }

  // All validations passed, apply writes
  WriteOptions write_opts;
  s = db_->Write(write_opts, &write_batch_);
  
  if (s.ok()) {
    state_ = TransactionState::COMMITTED;
    // Clear buffers
    write_buffer_.clear();
    write_batch_.Clear();
  } else {
    Abort();  // Rollback on failure
  }
  
  return s;
}

Status Transaction::Abort() {
  if (state_ == TransactionState::COMMITTED) {
    return Status::InvalidArgument("Cannot rollback: transaction already committed");
  }

  if (state_ == TransactionState::ABORTED) {
    return Status::OK();  // Already aborted
  }

  state_ = TransactionState::ABORTED;
  write_buffer_.clear();
  write_batch_.Clear();
  
  return Status::OK();
}

}  // namespace leveldb