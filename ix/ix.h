#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <memory>
#include <climits>
#include <bitset>
#include <string.h>
#include <math.h>
#include <stdexcept>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <cstring>
#include <algorithm>
#include <memory>

#include "../rbf/rbfm.h"

#define IX_EOF (-1) // end of the index scan
#define INDEX_OCCUPANCY_D 50
#define NEXT_PAGE_POINTER_SIZE 4
#define NULL_INDICATOR (-1)
#define LEAF_NODE_INDICATOR_SIZE 1
#define SLOT_NUMBER_POINTER_IX 4

class IX_ScanIterator;
class IXFileHandle;
class IX_Page;
class IX_LeafNode;
class IX_NonLeafNode;

typedef struct
{
  unsigned slotOffset;
  unsigned slotLen;
} SlotEntry;


class IXFileHandle
{
public:
  // variables to keep counter for each operation
  unsigned ixReadPageCounter;
  unsigned ixWritePageCounter;
  unsigned ixAppendPageCounter;

  // Constructor
  IXFileHandle();

  // Destructor
  ~IXFileHandle();
  RC updateMetadata();
  RC readMetadata();
  bool iSscanIteratorOpen();
  // Put the current counter values of associated PF FileHandles into variables
  RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

public:
  FileHandle fh;
  PageNum root;
  unsigned occupancy;
  RID currentRid;
};

class IndexManager
{

public:
  static IndexManager *instance();

  // Create an index file.
  RC createFile(const string &fileName);

  // Delete an index file.
  RC destroyFile(const string &fileName);

  // Open an index and return an ixfileHandle.
  RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

  // Close an ixfileHandle for an index.
  RC closeFile(IXFileHandle &ixfileHandle);

  // Insert an entry into the given index that is indicated by the given ixfileHandle.
  RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Delete an entry from the given index that is indicated by the given ixfileHandle.
  RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Initialize and IX_ScanIterator to support a range search
  RC scan(IXFileHandle &ixfileHandle,
          const Attribute &attribute,
          const void *lowKey,
          const void *highKey,
          bool lowKeyInclusive,
          bool highKeyInclusive,
          IX_ScanIterator &ix_ScanIterator);

  // Print the B+ tree in pre-order (in a JSON record format)
  void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

private:
  RC deleteRoutine(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, 
                    void* parentPage, const PageNum &parentPageNum, void *page, const PageNum &pageNum, IX_NonLeafNode &oldChild);
  RC deletedLeafNode(const Attribute &attribute, void* page, IX_LeafNode &node);
  RC deletedNonLeafNode(const Attribute &attribute, void* page, IX_NonLeafNode &node);
  RC redistributeLeafPage(const Attribute &attribute, const unsigned &occupancyThreshold, void *page,
                                      void *siblingPage, const PageNum &siblingPageNum, void *parentPage);
  RC redistributeNonLeafPage(const Attribute &attribute, const unsigned &occupancyThreshold, void *page,
                                         void *siblingPage, const PageNum &siblingPageNum, void *parentPage);
  RC mergeLeafNode(const Attribute &attribute, void *page, const PageNum &siblingPageNum,
                               void *siblingPage, void *parentPage, IX_NonLeafNode &oldChild);
  RC mergeNonLeafNode(const Attribute &attribute, void *page, const PageNum &siblingPageNum,
                                  void *siblingPage, void *parentPage, IX_NonLeafNode &oldChild);

  void printRoutine(IXFileHandle &ixfileHandle, const Attribute &attribute, void* page, const PageNum &pageNum, string &tree) const;

  RC search(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, RID &rid);
  RC searchRoutine(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, void *page, PageNum &pageNum);
  RC findPage(const Attribute &attribute, const void *key, const RID &rid, const void *page, PageNum &returnPageNum);
  RC findSlotNum(const Attribute &attribute, const void *key, const void* page, unsigned &slotNum);
  RC getRootPage(IXFileHandle &ixfileHandle, void *page) const;

  RC insertRoutine(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, 
                   const RID &rid, void *page, const PageNum &pageNum, IX_NonLeafNode &newChild);
  RC insertNonLeafNode(const Attribute &attribute, void *page, IX_NonLeafNode &node);
  RC insertLeafNode(const Attribute &attribute, void *page, IX_LeafNode &node);
  RC splitNonLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page,
                      IX_NonLeafNode &incomingNode, IX_NonLeafNode &outgoingNode);
  RC splitLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page,
                   IX_LeafNode &incomingNode, IX_NonLeafNode &outgoingNode);
  RC getAffectedSlotNumForLeafPage(IX_Page &pageFormat, void *page, const Attribute &attribute, 
                                   IX_LeafNode &node, unsigned &slotNum);
  RC getAffectedSlotNumForNonLeafPage(IX_Page &pageFormat, void *page, const Attribute &attribute, 
                                      IX_NonLeafNode &node, unsigned &slotNum);

  RC initializeLeafPage(void* page);
  RC initializeNonLeafPage(void* page, PageNum defaultLeftPagePointer);
  RC getSiblingPage(void *page, const PageNum &pageNum, void *parentPage, PageNum &siblingPageNum, bool &isLeft);
  RC getPageEndPointer(IX_Page &pageFormat, const void *page, unsigned &endPointer);
  RC setNextPagePointer(void* page, PageNum &nextPageNum);
  bool IsLeafPage(const void *page);

protected:
  IndexManager();
  ~IndexManager();

private:
  static IndexManager *_index_manager;
  static PagedFileManager *pfm;
  static RecordBasedFileManager *rbfm;
};

class IX_ScanIterator
{
public:
  IndexManager* ix;
  //shared_ptr<IXFileHandle> ixfh;
  IXFileHandle ixfh;
  Attribute attribute;
  void* lowKey;
  void* highKey;
  bool lowKeyInclusive;
  bool highKeyInclusive;

  // Constructor
  IX_ScanIterator();

  // Destructor
  ~IX_ScanIterator();

  RC initialize(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                void *lowKey,
                void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                RID &rid);

  // Get next matching entry
  RC getNextEntry(RID &rid, void *key);

  // Terminate index scan
  RC close();

private:
  RC returnKey(IX_LeafNode node, void *key, RID &rid);
  RC setNextRid(unsigned &slotNum, PageNum &pageNum, const unsigned &maxNumofSlots, const PageNum &nextPage );
};

class IX_Page
{
public:
  unsigned freeSpace;
  unsigned numberOfSlots;
  vector<SlotEntry> slotDirectory;
  PageNum nextPage;
  bool isLeaf;

  IX_Page(const void *page);
  ~IX_Page(){};
  RC updateHeader(void *page);
  RC deepLoadHeader(const void *page);
  RC updateNode(const Attribute &attribute, void* page, IX_NonLeafNode node, unsigned &slotNum);
};

class IX_LeafNode
{
public:
  RID rid;
  void *key;

  IX_LeafNode(const void *page, const SlotEntry &slotEntry);
  IX_LeafNode(const Attribute &attribute, const void *key, const RID &rid);
  IX_LeafNode();
  RC copyTo(IX_LeafNode &node);
  RC realize(const Attribute &attribute, string &nodeString);
  int compare(const Attribute &attribute, const void *key, const RID &rid);
  int compare(const Attribute &attribute, IX_LeafNode &node, bool keyOnly);
  int compare(const Attribute &attribute, const void *key);
  RC returnKey(const Attribute &attribute, void* key);
  ~IX_LeafNode();
  RC fabricateNode(void *keyRid);
  unsigned getNodeSize() const;

private:
  unsigned size;
  unsigned keySize;
};

class IX_NonLeafNode
{
public:
  IX_LeafNode keyRidPair;
  PageNum leftPage;
  PageNum rightPage;

  IX_NonLeafNode(const void *page, const SlotEntry &slotEntry);
  IX_NonLeafNode(IX_LeafNode &keyRidPair, PageNum &rightPage);
  RC copyTo(IX_NonLeafNode &node);
  IX_NonLeafNode();
  ~IX_NonLeafNode(){};
  RC fabricateNode(void *entry);
  unsigned getNodeSize() const;

private:
  unsigned size;
};

#endif
