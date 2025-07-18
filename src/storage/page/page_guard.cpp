//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <sys/mman.h>
#include <algorithm>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include "common/config.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  std::unique_lock<std::mutex> bpm_lock(*bpm_latch_);
  frame_->pin_count_.fetch_add(1);
  replacer_->SetEvictable(frame_->frame_id_, false);
  replacer_->RecordAccess(frame_->frame_id_);
  // 这里可以放心解锁，因为 frame 被设置为不可 evictable ，不会被修改
  bpm_lock.unlock();
  // 获取写锁
  read_lock_ = std::shared_lock<std::shared_mutex>(frame_->rwlatch_);
  is_valid_ = true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : page_id_(std::exchange(that.page_id_, INVALID_PAGE_ID)),
      frame_(std::exchange(that.frame_, nullptr)),
      replacer_(std::exchange(that.replacer_, nullptr)),
      bpm_latch_(std::exchange(that.bpm_latch_, nullptr)),
      disk_scheduler_(std::exchange(that.disk_scheduler_, nullptr)),
      is_valid_(std::exchange(that.is_valid_, false)),
      read_lock_(std::move(that.read_lock_)) {}
/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    this->Drop();

    page_id_ = std::exchange(that.page_id_, INVALID_PAGE_ID);
    frame_ = std::exchange(that.frame_, nullptr);
    replacer_ = std::exchange(that.replacer_, nullptr);
    bpm_latch_ = std::exchange(that.bpm_latch_, nullptr);
    disk_scheduler_ = std::exchange(that.disk_scheduler_, nullptr);
    is_valid_ = std::exchange(that.is_valid_, false);
    read_lock_ = std::move(that.read_lock_);
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Flush() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // NOTE: 这里应该没必要实现，因为这里只是读取数据，不会发生数据的改变
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  if (is_valid_) {
    is_valid_ = false;
    read_lock_.unlock();
    {
      std::lock_guard<std::mutex> lock(*bpm_latch_);
      frame_->pin_count_.fetch_sub(1);
      if (frame_->pin_count_.load() == 0) {
        replacer_->SetEvictable(frame_->frame_id_, true);
      }
    }
  }
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  // 这里加锁是因为防止有其他线程修改 frame 和 replacer
  std::unique_lock<std::mutex> bpm_lock(*bpm_latch_);
  // 因为这里创建了一个新的对象来操作 FrameHeader，因此 pin_count_ 需要自增，
  // 同时由于对象现在持有了这个 frame，所以这个 frame 不能被 Evict
  // 记录frame被访问过一次
  frame_->pin_count_.fetch_add(1);
  replacer_->SetEvictable(frame_->frame_id_, false);
  replacer_->RecordAccess(frame_->frame_id_);
  // 这里可以放心解锁，因为 frame 被设置为不可 evictable ，不会被修改
  bpm_lock.unlock();
  // 获取写锁
  write_lock_ = std::unique_lock<std::shared_mutex>(frame_->rwlatch_);
  is_valid_ = true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : page_id_(std::exchange(that.page_id_, INVALID_PAGE_ID)),
      frame_(std::exchange(that.frame_, nullptr)),
      replacer_(std::exchange(that.replacer_, nullptr)),
      bpm_latch_(std::exchange(that.bpm_latch_, nullptr)),
      disk_scheduler_(std::exchange(that.disk_scheduler_, nullptr)),
      is_valid_(std::exchange(that.is_valid_, false)),
      write_lock_(std::move(that.write_lock_)) {}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();

    page_id_ = std::exchange(that.page_id_, INVALID_PAGE_ID);
    frame_ = std::exchange(that.frame_, nullptr);
    replacer_ = std::exchange(that.replacer_, nullptr);
    bpm_latch_ = std::exchange(that.bpm_latch_, nullptr);
    disk_scheduler_ = std::exchange(that.disk_scheduler_, nullptr);
    is_valid_ = std::exchange(that.is_valid_, false);
    write_lock_ = std::move(that.write_lock_);
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Flush() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  if (frame_->is_dirty_) {
    auto promise = disk_scheduler_->CreatePromise();
    disk_scheduler_->Schedule({true, frame_->GetDataMut(), page_id_, std::move(promise)});
    frame_->is_dirty_ = false;
  }
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  if (is_valid_) {
    is_valid_ = false;
    write_lock_.unlock();
    {
      std::lock_guard<std::mutex> lock(*bpm_latch_);
      frame_->pin_count_.fetch_sub(1);
      if (frame_->pin_count_.load() == 0) {
        replacer_->SetEvictable(frame_->frame_id_, true);
      }
    }
  }
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
