#ifndef _rbfm_h_
#define _rbfm_h_

#define BYTE_SIZE 8
#define RECORD_NUMBER_OF_FIELD_SIZE 2
#define RECORD_FIELD_POINTER_SIZE 2
#define PAGE_NUMBER_POINTER 2
#define SLOT_NUMBER_POINTER 2
#define MAXXOUT_INDICATOR 65535
#define TOMBSTONE_SIZE 6
#include <string>
#include <vector>
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

#include "../rbf/pfm.h"

using namespace std;

// Record ID
typedef struct
{
  unsigned pageNum; // page number
  unsigned slotNum; // slot number in the page
} RID;

// Attribute
typedef enum
{
  TypeInt = 0,
  TypeReal,
  TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute
{
  string name;       // attribute name
  AttrType type;     // attribute type
  AttrLength length; // attribute length
};

typedef unsigned char ByteValue;

struct AttributeValuePair
{
  string name;         // attribute name
  AttrType type;       // attributfreeSpaceBuffere type
  AttrLength capacity; // attribute max capacity
  AttrLength length;   // attribute length
  ByteValue *value;    // attribute value
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum
{
  EQ_OP = 0, // no condition// =
  LT_OP,     // <
  LE_OP,     // <=
  GT_OP,     // >
  GE_OP,     // >=
  NE_OP,     // !=
  NO_OP      // no condition
} CompOp;

/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

#define RBFM_EOF (-1) // end of a scan operator

class RecordManager
{
public:
  RecordManager(const unsigned recordDescriptorSize);
  ~RecordManager(){};

  RC createData();
  RC understandData(const vector<Attribute> &recordDescriptor, const void *data);
  RC createRecord();
  RC understandRecord(const vector<Attribute> &recordDescriptor, const void *record);
  RC print();
  RC copyTo(RecordManager &recordManager);

public:
  unsigned numberOfFields;
  unsigned char *nullFlag;
  unsigned nullFlagSize;
  vector<AttributeValuePair> fieldValues;
  unsigned recordSize;
  unsigned rawDataSize;
  void *record;
  void *rawData;
};

class RecordBasedFileManager;

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator
{
public:
  RBFM_ScanIterator(){};

  ~RBFM_ScanIterator(){};
  RC initialize(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                const string &conditionedField, const CompOp compOp, const void *value,
                const vector<int> &attributes);

  // Never keep the results in the memory. When getNextRecord() is called,
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data); // { return RBFM_EOF; };
  RC close();                             // { return -1; };
  RC createData(const RecordManager recordManager, RID &rid, void *data, const int i, const int j);

public:
  RecordBasedFileManager *rbfm;
  FileHandle fileHandle;
  vector<Attribute> recordDescriptor;
  string conditionedField;
  CompOp compOp;
  void *value;
  vector<int> attributes;
  RID currentRID;
};

class RecordBasedFileManager
{
public:
  static RecordBasedFileManager *instance();

  RC createFile(const string &fileName);

  RC destroyFile(const string &fileName);

  RC openFile(const string &fileName, FileHandle &fileHandle);

  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first,
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

  // This method will be mainly used for debugging/testing.
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

  /******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  RC scan(FileHandle &fileHandle,
          const vector<Attribute> &recordDescriptor,
          const string &conditionAttribute,
          const CompOp compOp,                  // comparision type such as "<" and "="
          const void *value,                    // used in the comparison
          const vector<string> &attributeNames, // a list of projected attributes
          RBFM_ScanIterator &rbfm_ScanIterator);

  RC getSlotDirectoryEntry(const unsigned &slotNum, const void* pageBuffer, unsigned &slotOffset, unsigned &slotLen);
  RC getNumberOfSlots(const void *pageBuffer, unsigned &numberOfSlots);

private:
  RC getPageFreeSpace(const void *pageBuffer, unsigned &freeSpace);
  RC reducePageFreeSpace(const void *pageBuffer, const unsigned &freeSpace);
  RC increasePageFreeSpace(const void *pageBuffer, const unsigned &freeSpace);
  RC getNumberOfRecords(const void *pageBuffer, unsigned &numberOfRecords);
  RC updateSlotDirectory(const unsigned &slotNum, const unsigned &slotOffset, const unsigned &slotLen, void *pageBuffer);
  RC shiftRecordsToLeft(const unsigned &modifyingSlotNum, const unsigned &delta, void *pageBuffer);
  RC shiftRecordsToRight(const unsigned &modifyingSlotNum, const unsigned &delta, void *pageBuffer);
  RC initializePageWithMetadata(void *pageBuffer);
  RC getAvailableSlot(void* pageBuffer, unsigned &availableSlotNum);
  bool checkIfDeleted(const void* pageBuffer, const unsigned &slotNum);
  bool checkIfTombStone(const void* pageBuffer, const unsigned &slotNum, RID &updatedRid);
  RC updateFullSlotDirectory(const unsigned &modifyingSlotNum, void *pageBuffer, const unsigned &delta, const bool &negative);
  RC reduceNumberOfRecords(const void* pageBuffer);
  RC increaseNumberOfRecords(const void* pageBuffer);
  RC increaseNumberOfSlots(const void* pageBuffer);
  RC getPageEndPointer(const void* page, unsigned &endPointer);

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
  static PagedFileManager *pfm;
};

#endif
