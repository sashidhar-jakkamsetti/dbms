
#ifndef _rm_h_
#define _rm_h_

#include <map>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

const string TABLES_FILENAME  = "Tables.sys.tbl";
const string COLUMNS_FILENAME  = "Columns.sys.tbl";
const string TABLES_TABLENAME = "Tables";
const string COLUMNS_TABLENAME = "Columns";
const string TABLES_COLUMN_TABLE_ID = "table-id";
const string TABLES_COLUMN_TABLE_NAME = "table-name";
const string COLUMNS_COLUMN_COLUMN_NAME = "column-name";
const string TABLES_COLUMN_FILE_NAME  = "file-name";
const string COLUMNS_COLUMN_COLUMN_TYPE = "column-type";
const string TABLES_COLUMN_SYSTEM_FLAG  = "system-flag";
const string COLUMNS_COLUMN_COLUMN_LENGTH = "column-length";
const string COLUMNS_COLUMN_COLUMN_POSITION = "column-position";
const string COLUMNS_COLUMN_COLUMN_ISINDEXED = "column-isIndexed?";
const string INDEX_FILENAME_SUFFIX = ".idx";
#define TABLES_TABLE_ID 1
#define COLUMNS_TABLE_ID 2
#define MAX_DATA_SIZE_COLUMNS_TABLE 72
#define MAX_DATA_SIZE_TABLES_TABLE 117
#define NUMBER_OF_FIELDS_TABLES_TABLE 4
#define NUMBER_OF_FIELDS_COLUMNS_TABLE 6

// System Flag
typedef enum
{
  System = 0,
  User
} SystemFlag;

using namespace std;

#define RM_EOF (-1) // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator
{
public:
  RM_ScanIterator(){};
  ~RM_ScanIterator(){};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();

public:
  RBFM_ScanIterator rbfmsi;
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() {};  	// Constructor
  ~RM_IndexScanIterator() {}; 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key); 	// Get next matching entry
  RC close();       			// Terminate index scan

public:
  IX_ScanIterator ixsi;
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager *instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
          const string &conditionAttribute,
          const CompOp compOp,                  // comparison type such as "<" and "="
          const void *value,                    // used in the comparison
          const vector<string> &attributeNames, // a list of projected attributes
          RM_ScanIterator &rm_ScanIterator);
  
  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);

private:
  RC prepareRecordForTablesTable(const unsigned &tableId, const string &tableName, const string &fileName,
                                 const SystemFlag &systemFlag, RecordManager &recordManager);
  RC prepareRecordForColumnsTable(const unsigned &tableId, const string &tableName, const unsigned &columnType,
                                  const unsigned &columnLength, const unsigned &columnPosition, 
                                  const unsigned &isIndexed, RecordManager &recordManager);
  RC getSchemaColumnsTable(vector<Attribute> &attributes);
  RC getSchemaTablesTable(vector<Attribute> &attributes);
  RC initializeSystemColumnsTable(FileHandle &fileHandle);
  RC initializeSystemTablesTable(FileHandle &fileHandle);
  RC insertInSystemTable(const string &tableName, const string &tableFileName, unsigned &tableId);
  RC insertInSystemColumn(const unsigned &tableId, const vector<Attribute> &attributes);
  RC deleteFromSystemTable(const string &tableName, unsigned &tableId, string &tableFileName, SystemFlag &flag);
  RC deleteFromSystemColumn(const string &tableName, const unsigned &tableId, const SystemFlag &flag);
  RC getMaxTableId(unsigned &tableId);
  RC getTableInfo(const string &tableName, unsigned &tableId, string &tableFileName, SystemFlag &flag, RID &rid);
  RC getSchemaUserTable(const unsigned &tableId, vector<RID> &rids);
  RC convertRecordToAttribute(const vector<Attribute> recordDescriptor, 
                              const void *record, Attribute &attr, unsigned &position, unsigned &isIndexed);
  vector<Attribute> getIndicesOfTable(const string &tableName);
  RC prepareKeyForIndex(const vector<Attribute> recordDescriptor, const Attribute &attribute, const void* data, void* key);
  // Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr){return -1;}

  RC dropAttribute(const string &tableName, const string &attributeName){return -1;}

protected:
  RelationManager();
  ~RelationManager();

private:
  static RecordBasedFileManager *rbfm;
  static IndexManager *ix;
};

#endif
