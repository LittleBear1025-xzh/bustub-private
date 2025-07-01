//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include <utility>
#include "common/config.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  if (IsEmpty()) {
    return false;
  }

  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t current_page_id = header_page->root_page_id_;

  ctx.read_set_.emplace_back(std::move(header_page));

  while (current_page_id != INVALID_PAGE_ID) {
    ReadPageGuard current_guard = bpm_->ReadPage(current_page_id);
    auto current_page = current_guard.As<BPlusTreePage>();
    if (current_page->IsLeafPage()) {
      const LeafPage *leaf_page = current_guard.As<LeafPage>();
      ValueType value;
      if (leaf_page->Lookup(key, value, comparator_)) {
        result->emplace_back(value);
        return true;
      }
      return false;
    }

    // 如果是内部节点
    const InternalPage *internal_page = current_guard.As<InternalPage>();
    current_page_id = internal_page->Lookup(key, comparator_);
    // 在 context 中记下读取记录
    ctx.read_set_.emplace_back(std::move(current_guard));
  }
  return false;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  InitContext(ctx);
  if (IsEmpty()) {
    InitTree();
  }
  LeafPage *leaf_page = FindInsertedLeafPageAndRecordSearchPath(ctx, key);

  InsertInToLeafPage(ctx, leaf_page, key, value);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InitTree() {
  page_id_t page_id = bpm_->NewPage();

  WritePageGuard header_guard = bpm_->WritePage(header_page_id_);
  BPlusTreeHeaderPage *header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = page_id;

  WritePageGuard root_guard = bpm_->WritePage(page_id);
  LeafPage *root_page = root_guard.AsMut<LeafPage>();
  root_page->Init(leaf_max_size_);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertInToLeafPage(Context &context, LeafPage *leaf_page, const KeyType &key,
                                        const ValueType &value) {
  if (!leaf_page->Insert(key, value, comparator_)) {
    Split(context, leaf_page);
    leaf_page = FindInsertedLeafPageAndRecordSearchPath(context, key);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindInsertedLeafPageAndRecordSearchPath(Context &context, const KeyType &key) const -> LeafPage * {
  page_id_t current_page_id;
  if (context.write_set_.empty()) {
    current_page_id = context.root_page_id_;
  } else {
    current_page_id = context.write_set_.back().GetPageId();
    context.write_set_.pop_back();
  }
  auto current_guard = bpm_->WritePage(current_page_id);
  const BPlusTreePage *current_page = current_guard.As<BPlusTreePage>();
  while (!current_page->IsLeafPage()) {
    const InternalPage *internal_page = current_guard.As<InternalPage>();
    current_page_id = internal_page->FindInsertedPage(key, comparator_);
    context.write_set_.emplace_back(std::move(current_guard));
    current_guard = bpm_->WritePage(current_page_id);
    current_page = current_guard.As<BPlusTreePage>();
  }
  return current_guard.AsMut<LeafPage>();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InitContext(Context &context) {
  context.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = context.header_page_->As<BPlusTreeHeaderPage>();
  context.root_page_id_ = header_page->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Split(Context &context, LeafPage *old_leaf_page) {
  KeyType mid_key = std::move(SplitLeafNode(old_leaf_page));
  InternalPage *internael_page = context.write_set_.back().AsMut<InternalPage>();
  // TODO:context.write_set_.pop_back();
  if (!internael_page->InsertKey(mid_key)) {
    SplitUpward();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafNode(LeafPage *old_leaf_page) -> KeyType {
  return old_leaf_page->KeyAt(old_leaf_page->GetMaxSize() / 2);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { UNIMPLEMENTED("TODO(P2): Add implementation."); }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
