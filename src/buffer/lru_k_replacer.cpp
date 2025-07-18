//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <climits>
#include <cstddef>
#include <iterator>
#include <list>
#include <mutex>
#include <optional>
#include <utility>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return frame_id if a frame is evicted successfully, nullopt if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::unique_lock<std::mutex> lock(latch_);
  current_timestamp_ += 1;

  if (curr_size_ == 0) {
    return std::nullopt;
  }

  // 先扫描 less_k_node_list_
  for (auto frame_id : less_k_node_list_) {
    auto &node = node_store_[frame_id];
    if (node.IsEvictable()) {
      RemoveInternal(frame_id);
      return frame_id;
    }
  }

  // 再扫描 more_k_node_list_
  for (auto frame_id : more_k_node_list_) {
    auto &node = node_store_[frame_id];
    if (node.IsEvictable()) {
      RemoveInternal(frame_id);
      return frame_id;
    }
  }

  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // LOG_INFO("RecordAccess(frame_id: %d)", frame_id);
  // LOG_INFO("currnet more_k_node_list_:");
  // for (auto temp_frame_id : more_k_node_list_) {
  //   LOG_INFO("%d", temp_frame_id);
  // }
  std::unique_lock<std::mutex> lock(latch_);
  current_timestamp_ += 1;
  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode new_node(k_, frame_id);
    new_node.AddTimestamp(current_timestamp_);
    if (curr_size_ == replacer_size_) {
      lock.unlock();
      Evict();
      lock.lock();
    }
    node_store_.emplace(frame_id, new_node);
    less_k_node_list_.emplace_back(frame_id);
    less_k_node_map_[frame_id] = std::prev(less_k_node_list_.end());
  } else {
    LRUKNode *lru_k_node = &node_store_[frame_id];
    size_t old_size = lru_k_node->AddTimestamp(current_timestamp_);
    if (old_size < k_ - 1) {
      less_k_node_list_.erase(less_k_node_map_[frame_id]);
      less_k_node_list_.emplace_back(frame_id);
      less_k_node_map_[frame_id] = std::prev(less_k_node_list_.end());
    } else {
      if (old_size == k_ - 1) {
        less_k_node_list_.erase(less_k_node_map_[frame_id]);
        less_k_node_map_.erase(frame_id);
      } else {
        more_k_node_list_.erase(more_k_node_map_[frame_id]);
      }
      auto it = more_k_node_list_.begin();
      while (it != more_k_node_list_.end() &&
             node_store_[*it].GetKthHistory() <= node_store_[frame_id].GetKthHistory()) {
        // LOG_INFO("frame id: %d, k-th: %zu < frame id: %d, k-th: %zu", *it, node_store_[*it].GetKthHistory(),
        // frame_id,
        //          node_store_[frame_id].GetKthHistory());
        it++;
      }
      more_k_node_map_[frame_id] = more_k_node_list_.insert(it, frame_id);
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_ += 1;

  if (node_store_.find(frame_id) == node_store_.end()) return;

  LRUKNode *lru_k_node = &node_store_[frame_id];

  if (lru_k_node->IsEvictable() != set_evictable) {
    curr_size_ += (set_evictable ? 1 : -1);
  }
  lru_k_node->SetEvictable(set_evictable);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  RemoveInternal(frame_id);
}

void LRUKReplacer::RemoveInternal(frame_id_t frame_id) {
  current_timestamp_ += 1;

  if (node_store_.find(frame_id) == node_store_.end()) return;

  LRUKNode *lru_k_node = &node_store_[frame_id];

  if (!lru_k_node->IsEvictable()) return;

  if (lru_k_node->GetHistorySize() < k_) {
    less_k_node_list_.erase(less_k_node_map_[frame_id]);
    less_k_node_map_.erase(frame_id);
  } else {
    more_k_node_list_.erase(more_k_node_map_[frame_id]);
    more_k_node_map_.erase(frame_id);
  }

  node_store_.erase(frame_id);
  curr_size_ -= 1;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
