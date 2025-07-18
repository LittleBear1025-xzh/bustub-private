//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 public:
  void SetEvictable(bool set_evictable) { this->is_evictable_ = set_evictable; }
  bool IsEvictable() const { return is_evictable_; }

  /**
   * @return the history's size before add timestamp
   * */
  size_t AddTimestamp(size_t timestamp) {
    size_t old_size = history_.size();
    if (old_size >= k_) {
      history_.pop_back();
    }
    history_.emplace_front(timestamp);
    return old_size;
  }

  frame_id_t GetFrameId() const { return fid_; }
  size_t GetHistorySize() const { return history_.size(); }
  size_t GetKthHistory() const { return history_.back(); }

  LRUKNode() {}
  LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  std::list<size_t> history_;
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  void RemoveInternal(frame_id_t frame_id);
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::unordered_map<frame_id_t, LRUKNode> node_store_;
  std::list<frame_id_t> less_k_node_list_;
  std::list<frame_id_t> more_k_node_list_;

  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> less_k_node_map_;
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> more_k_node_map_;
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
};

}  // namespace bustub
