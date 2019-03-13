
#include "rm.h"

RecordBasedFileManager *RelationManager::rbfm = RecordBasedFileManager::instance();
IndexManager *RelationManager::ix = IndexManager::instance();

RelationManager *RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

inline string getIndexFileName(const string &tableName, const string &attributeName)
{
    return tableName + "." + attributeName + INDEX_FILENAME_SUFFIX;
}

RC RelationManager::createCatalog()
{
    FileHandle tablesFileHandle, columnsFileHandle;

    if (rbfm->createFile(COLUMNS_FILENAME) == 0 && rbfm->createFile(TABLES_FILENAME) == 0)
    {
        if (rbfm->openFile(COLUMNS_FILENAME, columnsFileHandle) == 0)
        {
            initializeSystemColumnsTable(columnsFileHandle);
            rbfm->closeFile(columnsFileHandle);

            if (rbfm->openFile(TABLES_FILENAME, tablesFileHandle) == 0)
            {
                initializeSystemTablesTable(tablesFileHandle);
                rbfm->closeFile(tablesFileHandle);

                return 0;
            }
        }
    }

    return -1;
}

RC RelationManager::initializeSystemTablesTable(FileHandle &fileHandle)
{
    RID rid;
    vector<Attribute> attributes;
    if (getAttributes(TABLES_TABLENAME, attributes) == 0)
    {
        int error = 0;
        RecordManager recordManager(NUMBER_OF_FIELDS_TABLES_TABLE);

        prepareRecordForTablesTable(TABLES_TABLE_ID, TABLES_TABLENAME, TABLES_FILENAME, System, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);

        prepareRecordForTablesTable(COLUMNS_TABLE_ID, COLUMNS_TABLENAME, COLUMNS_FILENAME, System, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);

        return error == 0 ? 0 : -1;
    }
    return -1;
}

RC RelationManager::initializeSystemColumnsTable(FileHandle &fileHandle)
{
    RID rid;
    vector<Attribute> attributes;
    if (getAttributes(COLUMNS_TABLENAME, attributes) == 0)
    {
        RecordManager recordManager(NUMBER_OF_FIELDS_COLUMNS_TABLE);
        int error = 0;
        unsigned defaultIndexStatus = 0;

        prepareRecordForColumnsTable(TABLES_TABLE_ID, TABLES_COLUMN_TABLE_ID, TypeInt, 4, 1, defaultIndexStatus, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);
        prepareRecordForColumnsTable(TABLES_TABLE_ID, TABLES_COLUMN_TABLE_NAME, TypeVarChar, 50, 2, defaultIndexStatus, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);
        prepareRecordForColumnsTable(TABLES_TABLE_ID, TABLES_COLUMN_FILE_NAME, TypeVarChar, 50, 3, defaultIndexStatus, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);
        prepareRecordForColumnsTable(TABLES_TABLE_ID, TABLES_COLUMN_SYSTEM_FLAG, TypeInt, 4, 4, defaultIndexStatus, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);
        prepareRecordForColumnsTable(COLUMNS_TABLE_ID, TABLES_COLUMN_TABLE_ID, TypeInt, 4, 1, defaultIndexStatus, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);
        prepareRecordForColumnsTable(COLUMNS_TABLE_ID, COLUMNS_COLUMN_COLUMN_NAME, TypeVarChar, 50, 2, defaultIndexStatus, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);
        prepareRecordForColumnsTable(COLUMNS_TABLE_ID, COLUMNS_COLUMN_COLUMN_TYPE, TypeInt, 4, 3, defaultIndexStatus, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);
        prepareRecordForColumnsTable(COLUMNS_TABLE_ID, COLUMNS_COLUMN_COLUMN_LENGTH, TypeInt, 4, 4, defaultIndexStatus, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);
        prepareRecordForColumnsTable(COLUMNS_TABLE_ID, COLUMNS_COLUMN_COLUMN_POSITION, TypeInt, 4, 5, defaultIndexStatus, recordManager);
        error += rbfm->insertRecord(fileHandle, attributes, recordManager.rawData, rid);
        free(recordManager.rawData);
        return error == 0 ? 0 : -1;
    }
    return -1;
}

RC RelationManager::deleteCatalog()
{
    return (rbfm->destroyFile(TABLES_FILENAME) == 0 && rbfm->destroyFile(COLUMNS_FILENAME) == 0) ? 0 : -1;
}

RC RelationManager::getMaxTableId(unsigned &tableId)
{
    int error = -1;
    RID rid;
    vector<Attribute> tableAttributes;
    if (getAttributes(TABLES_TABLENAME, tableAttributes) == 0)
    {
        FileHandle tablesFileHandle;
        if (rbfm->openFile(TABLES_FILENAME, tablesFileHandle) == 0)
        {
            string searchOn = TABLES_COLUMN_TABLE_ID;
            vector<string> extractAttributes;
            extractAttributes.push_back(TABLES_COLUMN_TABLE_ID);
            unsigned startTableId = COLUMNS_TABLE_ID;
            unsigned maxTableId = startTableId;
            unsigned nextTableId = maxTableId;

            RBFM_ScanIterator rbfmsi;
            if (rbfm->scan(tablesFileHandle, tableAttributes, searchOn, GE_OP, &startTableId, extractAttributes, rbfmsi) == 0)
            {
                void *scannedData = malloc(5);

                while (rbfmsi.getNextRecord(rid, scannedData) != RM_EOF)
                {
                    nextTableId = *(int *)((char *)scannedData + 1);

                    if (nextTableId > maxTableId)
                    {
                        maxTableId = nextTableId;
                    }
                }

                rbfmsi.close();
                tableId = maxTableId;
                free(scannedData);

                error = 0;
            }
            rbfm->closeFile(tablesFileHandle);
        }
    }

    return error;
}

RC RelationManager::insertInSystemTable(const string &tableName, const string &tableFileName, unsigned &tableId)
{
    RID rid;
    if (getMaxTableId(tableId) == 0)
    {
        tableId += 1;
        vector<Attribute> tablesAttributes;
        if (getAttributes(TABLES_TABLENAME, tablesAttributes) == 0)
        {
            RecordManager recordManager(NUMBER_OF_FIELDS_TABLES_TABLE);
            prepareRecordForTablesTable(tableId, tableName, tableFileName, User, recordManager);

            FileHandle tablesFileHandle;
            int error = 0;
            if (rbfm->openFile(TABLES_FILENAME, tablesFileHandle) == 0)
            {
                error = rbfm->insertRecord(tablesFileHandle, tablesAttributes, recordManager.rawData, rid);
                free(recordManager.rawData);
                rbfm->closeFile(tablesFileHandle);
                return error;
            }
        }
    }
    return -1;
}

RC RelationManager::insertInSystemColumn(const unsigned &tableId, const vector<Attribute> &attributes)
{
    RID rid;
    vector<Attribute> columnAttributes;
    if (getAttributes(COLUMNS_TABLENAME, columnAttributes) == 0)
    {
        RecordManager recordManager(NUMBER_OF_FIELDS_COLUMNS_TABLE);

        FileHandle columnsFileHandle;
        unsigned defaultIndexStatus = 0;
        if (rbfm->openFile(COLUMNS_FILENAME, columnsFileHandle) == 0)
        {
            int error = 0;
            for (int i = 0; i < attributes.size(); i++)
            {
                prepareRecordForColumnsTable(tableId, attributes[i].name, attributes[i].type,
                                             attributes[i].length, (i + 1), defaultIndexStatus, recordManager);
                error += rbfm->insertRecord(columnsFileHandle, columnAttributes, recordManager.rawData, rid);
                free(recordManager.rawData);
            }

            rbfm->closeFile(columnsFileHandle);
            return error == 0 ? 0 : -1;
        }
    }

    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    unsigned tableId;
    string tableFileName = tableName + ".tbl";

    if (insertInSystemTable(tableName, tableFileName, tableId) == 0 && insertInSystemColumn(tableId, attrs) == 0)
    {
        return rbfm->createFile(tableFileName);
    }

    return -1;
}

RC RelationManager::getTableInfo(const string &tableName, unsigned &tableId, string &tableFileName, SystemFlag &flag, RID &rid)
{
    FileHandle tablesFileHandle;
    int error = -1;
    if (rbfm->openFile(TABLES_FILENAME, tablesFileHandle) == 0)
    {
        char *tableFileNameChar = (char *)malloc(50);
        memset((char *)tableFileNameChar, '\0', 50);

        unsigned tableNameStringLength = tableName.length();
        char *tableNameChar = (char *)malloc(tableNameStringLength + sizeof(int));
        memset((char *)tableNameChar, '\0', tableNameStringLength + sizeof(int));
        memcpy((char *)tableNameChar, &tableNameStringLength, sizeof(int));
        memcpy((char *)tableNameChar + sizeof(int), tableName.c_str(), tableNameStringLength);

        void *scannedData = malloc(MAX_DATA_SIZE_TABLES_TABLE);
        RBFM_ScanIterator rbfmsi;
        vector<Attribute> tableAttributes;
        if (getAttributes(TABLES_TABLENAME, tableAttributes) == 0)
        {
            string searchOn = TABLES_COLUMN_TABLE_NAME;
            vector<string> extractAttributes;
            extractAttributes.push_back(TABLES_COLUMN_TABLE_ID);
            extractAttributes.push_back(TABLES_COLUMN_TABLE_NAME);
            extractAttributes.push_back(TABLES_COLUMN_FILE_NAME);
            extractAttributes.push_back(TABLES_COLUMN_SYSTEM_FLAG);

            if (rbfm->scan(tablesFileHandle, tableAttributes, searchOn, EQ_OP, tableNameChar, extractAttributes, rbfmsi) == 0)
            {
                if (rbfmsi.getNextRecord(rid, scannedData) != RM_EOF)
                {
                    RecordManager recordManager(tableAttributes.size());

                    if (recordManager.understandData(tableAttributes, scannedData) == 0)
                    {
                        tableId = *((int *)recordManager.fieldValues[0].value);
                        flag = *((SystemFlag *)recordManager.fieldValues[3].value);
                        memcpy((char *)tableFileNameChar, (char *)recordManager.fieldValues[2].value, recordManager.fieldValues[2].length);

                        tableFileName = tableFileNameChar;
                        error = 0;
                        recordManager.createRecord();
                        free(recordManager.record);
                    }
                }

                rbfmsi.close();
            }
        }

        free(tableNameChar);
        free(tableFileNameChar);
        free(scannedData);
        rbfm->closeFile(tablesFileHandle);
    }

    return error;
}

RC RelationManager::deleteFromSystemTable(const string &tableName,
                                          unsigned &tableId, string &tableFileName, SystemFlag &flag)
{
    RID rid;
    if (getTableInfo(tableName, tableId, tableFileName, flag, rid) == 0)
    {
        if (flag != System)
        {
            vector<Attribute> tableAttributes;
            if (getAttributes(TABLES_TABLENAME, tableAttributes) == 0)
            {
                FileHandle tablesFileHandle;
                int error = 0;
                if (rbfm->openFile(TABLES_FILENAME, tablesFileHandle) == 0)
                {
                    error = rbfm->deleteRecord(tablesFileHandle, tableAttributes, rid);
                    rbfm->closeFile(tablesFileHandle);
                    return error;
                }
            }
        }
    }

    return -1;
}

RC RelationManager::getSchemaUserTable(const unsigned &tableId, vector<RID> &rids)
{
    int error = -1;
    FileHandle columnsFileHandle;
    if (rbfm->openFile(COLUMNS_FILENAME, columnsFileHandle) == 0)
    {
        vector<Attribute> columnsAttributes;
        if (getAttributes(COLUMNS_TABLENAME, columnsAttributes) == 0)
        {
            RID rid;
            void *scannedData = malloc(5);
            RBFM_ScanIterator rbfmsi;
            string searchOn = TABLES_COLUMN_TABLE_ID;
            vector<string> extractAttributes;
            extractAttributes.push_back(searchOn);
            if (rbfm->scan(columnsFileHandle, columnsAttributes, searchOn, EQ_OP, &tableId, extractAttributes, rbfmsi) == 0)
            {
                while (rbfmsi.getNextRecord(rid, scannedData) != RM_EOF)
                {
                    rids.push_back(rid);
                }

                error = 0;
                rbfmsi.close();
            }
            free(scannedData);
        }
        rbfm->closeFile(columnsFileHandle);
    }
    return error;
}

RC RelationManager::deleteFromSystemColumn(const string &tableName, const unsigned &tableId, const SystemFlag &flag)
{
    if (flag != System)
    {
        vector<RID> rids;
        if (getSchemaUserTable(tableId, rids) == 0)
        {
            vector<Attribute> indices = getIndicesOfTable(tableName);
            for (int i = 0; i < indices.size(); i++)
            {
                rbfm->destroyFile(getIndexFileName(tableName, indices[i].name));
            }

            vector<Attribute> columnsAttributes;
            if (getAttributes(COLUMNS_TABLENAME, columnsAttributes) == 0)
            {
                FileHandle columnsFileHandle;
                if (rbfm->openFile(COLUMNS_FILENAME, columnsFileHandle) == 0)
                {
                    int error = 0;
                    for (int i = 0; i < rids.size(); i++)
                    {
                        error += rbfm->deleteRecord(columnsFileHandle, columnsAttributes, rids[i]);
                    }
                    rbfm->closeFile(columnsFileHandle);
                    return error == 0 ? 0 : -1;
                }
            }
        }
    }

    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    unsigned tableId;
    string tableFileName;
    SystemFlag systemFlag;

    if (deleteFromSystemTable(tableName, tableId, tableFileName, systemFlag) == 0 && deleteFromSystemColumn(tableName, tableId, systemFlag) == 0)
    {
        if (systemFlag != System)
        {
            return rbfm->destroyFile(tableFileName);
        }
    }

    return -1;
}

RC RelationManager::convertRecordToAttribute(const vector<Attribute> recordDescriptor,
                                             const void *data, Attribute &attr, unsigned &position, unsigned &isIndexed)
{
    RecordManager recordManager(recordDescriptor.size());

    if (recordManager.understandData(recordDescriptor, data) == 0)
    {
        for (int i = 0; i < recordManager.fieldValues.size(); i++)
        {
            switch (recordManager.fieldValues[i].type)
            {
            case TypeVarChar:
            {
                unsigned char *fieldValueVarChar = (unsigned char *)malloc(recordManager.fieldValues[i].length + 1);
                memcpy(fieldValueVarChar, recordManager.fieldValues[i].value, recordManager.fieldValues[i].length);
                *((char *)fieldValueVarChar + recordManager.fieldValues[i].length) = '\0';
                if (recordManager.fieldValues[i].name == COLUMNS_COLUMN_COLUMN_NAME)
                {
                    attr.name = (char *)fieldValueVarChar;
                }
                free(fieldValueVarChar);
                break;
            }

            case TypeInt:
            {
                unsigned fieldValueInt = 0;
                memcpy(&fieldValueInt, recordManager.fieldValues[i].value, recordManager.fieldValues[i].length);

                if (recordManager.fieldValues[i].name == COLUMNS_COLUMN_COLUMN_TYPE)
                {
                    attr.type = (AttrType)fieldValueInt;
                }
                else if (recordManager.fieldValues[i].name == COLUMNS_COLUMN_COLUMN_LENGTH)
                {
                    attr.length = fieldValueInt;
                }
                else if (recordManager.fieldValues[i].name == COLUMNS_COLUMN_COLUMN_POSITION)
                {
                    position = fieldValueInt;
                }
                else if (recordManager.fieldValues[i].name == COLUMNS_COLUMN_COLUMN_ISINDEXED)
                {
                    isIndexed = fieldValueInt;
                }
                break;
            }

            default:
                break;
            }
        }
        recordManager.createRecord();
        free(recordManager.record);

        return 0;
    }

    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    if (tableName == TABLES_TABLENAME)
    {
        getSchemaTablesTable(attrs);
        return 0;
    }
    else if (tableName == COLUMNS_TABLENAME)
    {
        getSchemaColumnsTable(attrs);
        return 0;
    }
    else
    {
        unsigned tableId;
        string tableFileName = "";
        SystemFlag systemFlag;
        RID rid;
        if (getTableInfo(tableName, tableId, tableFileName, systemFlag, rid) != 0)
        {
            return -1;
        }

        vector<RID> rids;
        getSchemaUserTable(tableId, rids);

        vector<Attribute> columnsAttributes;
        getAttributes(COLUMNS_TABLENAME, columnsAttributes);

        FileHandle columnsFileHandle;
        if (rbfm->openFile(COLUMNS_FILENAME, columnsFileHandle) == 0)
        {
            Attribute attr;
            unsigned attrPosition = 0;
            unsigned isIndexed = 0;
            int error = 0;
            map<int, vector<Attribute>> positionAttributeMap;
            for (int i = 0; i < rids.size(); i++)
            {
                void *data = malloc(MAX_DATA_SIZE_COLUMNS_TABLE);
                error += rbfm->readRecord(columnsFileHandle, columnsAttributes, rids[i], data);
                error += convertRecordToAttribute(columnsAttributes, data, attr, attrPosition, isIndexed);
                free(data);
                if (positionAttributeMap.find(attrPosition) == positionAttributeMap.end())
                {
                    vector<Attribute> attributes;
                    attributes.push_back(attr);
                    positionAttributeMap.insert(pair<int, vector<Attribute>>(attrPosition, attributes));
                }
                else
                {
                    positionAttributeMap[attrPosition].push_back(attr);
                }
            }

            map<int, vector<Attribute>>::iterator it = positionAttributeMap.begin();
            for (it = positionAttributeMap.begin(); it != positionAttributeMap.end(); it++)
            {
                for (int i = 0; i < it->second.size(); i++)
                {
                    Attribute attr;
                    attr.name = it->second[i].name;
                    attrs.push_back(it->second[i]);
                }
            }

            rbfm->closeFile(columnsFileHandle);

            return error == 0 ? 0 : -1;
        }
    }

    return -1;
}

RC RelationManager::prepareKeyForIndex(const vector<Attribute> recordDescriptor,
                                       const Attribute &attribute, const void *data, void *key)
{
    RecordManager recordManager(recordDescriptor.size());

    if (recordManager.understandData(recordDescriptor, data) == 0)
    {
        for (int i = 0; i < recordManager.fieldValues.size(); i++)
        {
            if (recordManager.fieldValues[i].name == attribute.name)
            {
                if (recordManager.fieldValues[i].length == -1)
                {
                    return -1;
                }

                int pointer = 0;
                if (recordManager.fieldValues[i].type == TypeVarChar)
                {
                    memcpy((char *)key + pointer, &recordManager.fieldValues[i].length, sizeof(int));
                    pointer += sizeof(int);
                }

                memcpy((char *)key + pointer, recordManager.fieldValues[i].value, recordManager.fieldValues[i].length);
                return 0;
            }
        }
    }

    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    unsigned tableId;
    string tableFileName;
    SystemFlag flag = User;
    RID ridDummy;
    if (getTableInfo(tableName, tableId, tableFileName, flag, ridDummy) != 0)
    {
        return -1;
    }

    if (flag != System)
    {
        vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == 0)
        {
            FileHandle fileHandle;
            if (rbfm->openFile(tableFileName, fileHandle) == 0)
            {
                if (rbfm->insertRecord(fileHandle, recordDescriptor, data, rid) == 0)
                {
                    vector<Attribute> indices = getIndicesOfTable(tableName);

                    for (int i = 0; i < indices.size(); i++)
                    {
                        IXFileHandle ixFileHandle;
                        if (ix->openFile(getIndexFileName(tableName, indices[i].name), ixFileHandle) == 0)
                        {
                            int keyDataSize = 0;
                            if (indices[i].type == TypeVarChar)
                            {
                                keyDataSize += sizeof(int);
                            }
                            keyDataSize += indices[i].length;

                            void *key = malloc(keyDataSize);
                            memset(key, 0, keyDataSize);

                            if (prepareKeyForIndex(recordDescriptor, indices[i], data, key) == 0)
                            {
                                if (ix->insertEntry(ixFileHandle, indices[i], key, rid) != 0)
                                {
                                    rbfm->closeFile(fileHandle);
                                    ix->closeFile(ixFileHandle);
                                    free(key);
                                    return -1;
                                }
                            }

                            ix->closeFile(ixFileHandle);
                            free(key);
                        }
                    }

                    rbfm->closeFile(fileHandle);
                    return 0;
                }

                rbfm->closeFile(fileHandle);
            }
        }
    }

    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    unsigned tableId;
    string tableFileName;
    SystemFlag flag;
    RID ridDummy;
    if (getTableInfo(tableName, tableId, tableFileName, flag, ridDummy) != 0)
    {
        return -1;
    }

    if (flag != System)
    {
        vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == 0)
        {
            FileHandle fileHandle;
            if (rbfm->openFile(tableFileName, fileHandle) == 0)
            {
                void *data = malloc(PAGE_SIZE);
                memset(data, 0, PAGE_SIZE);
                if (rbfm->readRecord(fileHandle, recordDescriptor, rid, data) == 0)
                {
                    if (rbfm->deleteRecord(fileHandle, recordDescriptor, rid) == 0)
                    {
                        vector<Attribute> indices = getIndicesOfTable(tableName);

                        for (int i = 0; i < indices.size(); i++)
                        {
                            IXFileHandle ixFileHandle;
                            if (ix->openFile(getIndexFileName(tableName, indices[i].name), ixFileHandle) == 0)
                            {
                                int keyDataSize = 0;
                                if (indices[i].type == TypeVarChar)
                                {
                                    keyDataSize += sizeof(int);
                                }
                                keyDataSize += indices[i].length;

                                void *key = malloc(keyDataSize);
                                memset(key, 0, keyDataSize);

                                if (prepareKeyForIndex(recordDescriptor, indices[i], data, key) == 0)
                                {
                                    if (ix->deleteEntry(ixFileHandle, indices[i], key, rid) != 0)
                                    {
                                        rbfm->closeFile(fileHandle);
                                        ix->closeFile(ixFileHandle);
                                        free(data);
                                        free(key);
                                        return -1;
                                    }
                                }
                                ix->closeFile(ixFileHandle);
                                free(key);
                            }
                        }

                        rbfm->closeFile(fileHandle);
                        free(data);
                        return 0;
                    }
                }
                free(data);
                rbfm->closeFile(fileHandle);
            }
        }
    }

    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    unsigned tableId;
    string tableFileName;
    SystemFlag flag;
    RID ridDummy;
    if (getTableInfo(tableName, tableId, tableFileName, flag, ridDummy) != 0)
    {
        return -1;
    }

    if (flag != System)
    {
        vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == 0)
        {
            FileHandle fileHandle;
            if (rbfm->openFile(tableFileName, fileHandle) == 0)
            {
                RC rc = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
                rbfm->closeFile(fileHandle);
                return rc;
            }
        }
    }

    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    unsigned tableId;
    string tableFileName;
    SystemFlag flag;
    RID ridDummy;
    if (getTableInfo(tableName, tableId, tableFileName, flag, ridDummy) != 0)
    {
        return -1;
    }

    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == 0)
    {
        FileHandle fileHandle;
        if (rbfm->openFile(tableFileName, fileHandle) == 0)
        {
            RC rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
            rbfm->closeFile(fileHandle);
            return rc;
        }
    }

    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    unsigned tableId;
    string tableFileName;
    SystemFlag flag;
    RID ridDummy;
    if (getTableInfo(tableName, tableId, tableFileName, flag, ridDummy) != 0)
    {
        return -1;
    }

    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == 0)
    {
        FileHandle fileHandle;
        if (rbfm->openFile(tableFileName, fileHandle) == 0)
        {
            RC rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
            rbfm->closeFile(fileHandle);
            return rc;
        }
    }

    return -1;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    unsigned tableId = 0;
    string tableFileName;
    SystemFlag flag;
    RID ridDummy;
    if (getTableInfo(tableName, tableId, tableFileName, flag, ridDummy) != 0)
    {
        return -1;
    }

    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == 0)
    {
        FileHandle fileHandle;
        if (rbfm->openFile(tableFileName, fileHandle) == 0)
        {
            RC rc = rbfm->scan(fileHandle,
                               recordDescriptor,
                               conditionAttribute,
                               compOp,
                               value,
                               attributeNames,
                               rm_ScanIterator.rbfmsi);
            return rc;
        }
    }

    return -1;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfmsi.getNextRecord(rid, data);
}

RC RM_ScanIterator::close()
{
    return rbfmsi.close() == 0 && rbfmsi.rbfm->closeFile(rbfmsi.fileHandle) == 0;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
    unsigned tableId;
    string tableFileName;
    SystemFlag flag = User;
    RID ridDummy;
    if (getTableInfo(tableName, tableId, tableFileName, flag, ridDummy) != 0)
    {
        return -1;
    }

    if (flag != System)
    {
        vector<RID> rids;
        getSchemaUserTable(tableId, rids);

        vector<Attribute> columnsAttributes;
        getAttributes(COLUMNS_TABLENAME, columnsAttributes);

        FileHandle columnsFileHandle;
        if (rbfm->openFile(COLUMNS_FILENAME, columnsFileHandle) == 0)
        {
            Attribute attr;
            unsigned attrPosition = 0;
            unsigned isIndexed = 0;
            for (int i = 0; i < rids.size(); i++)
            {
                void *data = malloc(MAX_DATA_SIZE_COLUMNS_TABLE);
                if (rbfm->readRecord(columnsFileHandle, columnsAttributes, rids[i], data) == 0 && convertRecordToAttribute(columnsAttributes, data, attr, attrPosition, isIndexed) == 0)
                {
                    if (attr.name == attributeName && isIndexed == 0)
                    {
                        RecordManager recordManager(NUMBER_OF_FIELDS_COLUMNS_TABLE);

                        prepareRecordForColumnsTable(tableId, attr.name, attr.type, attr.length,
                                                     attrPosition, (unsigned)1, recordManager);
                        if (ix->createFile(getIndexFileName(tableName, attr.name)) == 0 && rbfm->updateRecord(columnsFileHandle, columnsAttributes, recordManager.rawData, rids[i]) == 0)
                        {
                            RM_ScanIterator rmsi;
                            vector<string> reqAttributes;
                            reqAttributes.push_back(attr.name);
                            IXFileHandle ixFileHandle;
                            if (scan(tableName, "", NO_OP, NULL, reqAttributes, rmsi) == 0 && ix->openFile(getIndexFileName(tableName, attr.name), ixFileHandle) == 0)
                            {
                                int returnDataSize = sizeof(char);
                                if (attr.type == TypeVarChar)
                                {
                                    returnDataSize += sizeof(int);
                                }
                                returnDataSize += attr.length;

                                RID returnedRid;
                                void *returnedData = malloc(returnDataSize);
                                memset(returnedData, 0, returnDataSize);

                                while (rmsi.getNextTuple(returnedRid, returnedData) != RM_EOF)
                                {
                                    unsigned nullIndicator = 0;
                                    memcpy(&nullIndicator, (char *)returnedData, sizeof(char));
                                    if (nullIndicator == 0)
                                    {
                                        if (ix->insertEntry(ixFileHandle, attr,
                                                            (char *)returnedData + sizeof(char), returnedRid) != 0)
                                        {
                                            rmsi.close();
                                            ix->closeFile(ixFileHandle);
                                            rbfm->closeFile(columnsFileHandle);
                                            free(returnedData);
                                            free(data);
                                            free(recordManager.rawData);
                                            return -1;
                                        }
                                    }
                                }
                                rmsi.close();
                                ix->closeFile(ixFileHandle);
                                rbfm->closeFile(columnsFileHandle);
                                free(returnedData);
                                free(data);
                                free(recordManager.rawData);
                                return 0;
                            }
                        }
                        free(recordManager.rawData);
                        break;
                    }
                }
                free(data);
            }

            rbfm->closeFile(columnsFileHandle);
        }
    }

    return -1;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
    unsigned tableId;
    string tableFileName;
    SystemFlag flag = User;
    RID ridDummy;
    if (getTableInfo(tableName, tableId, tableFileName, flag, ridDummy) != 0)
    {
        return -1;
    }

    if (flag != System)
    {
        vector<RID> rids;
        getSchemaUserTable(tableId, rids);

        vector<Attribute> columnsAttributes;
        getAttributes(COLUMNS_TABLENAME, columnsAttributes);

        FileHandle columnsFileHandle;
        if (rbfm->openFile(COLUMNS_FILENAME, columnsFileHandle) == 0)
        {
            Attribute attr;
            unsigned attrPosition = 0;
            unsigned isIndexed = 0;
            for (int i = 0; i < rids.size(); i++)
            {
                void *data = malloc(MAX_DATA_SIZE_COLUMNS_TABLE);
                if (rbfm->readRecord(columnsFileHandle, columnsAttributes, rids[i], data) == 0 && convertRecordToAttribute(columnsAttributes, data, attr, attrPosition, isIndexed) == 0)
                {
                    if (attr.name == attributeName && isIndexed == 1)
                    {
                        RecordManager recordManager(NUMBER_OF_FIELDS_COLUMNS_TABLE);

                        prepareRecordForColumnsTable(tableId, attr.name, attr.type, attr.length,
                                                     attrPosition, (unsigned)0, recordManager);
                        if (ix->destroyFile(getIndexFileName(tableName, attr.name)) == 0 && rbfm->updateRecord(columnsFileHandle, columnsAttributes, recordManager.rawData, rids[i]) == 0)
                        {
                            free(recordManager.rawData);
                            rbfm->closeFile(columnsFileHandle);
                            free(data);
                            return 0;
                        }
                        free(recordManager.rawData);
                        break;
                    }
                }
                free(data);
            }

            rbfm->closeFile(columnsFileHandle);
        }
    }

    return -1;
}

vector<Attribute> RelationManager::getIndicesOfTable(const string &tableName)
{
    vector<Attribute> indices;

    unsigned tableId;
    string tableFileName;
    SystemFlag flag = User;
    RID ridDummy;
    if (getTableInfo(tableName, tableId, tableFileName, flag, ridDummy) == 0)
    {
        vector<RID> rids;
        getSchemaUserTable(tableId, rids);

        vector<Attribute> columnsAttributes;
        getAttributes(COLUMNS_TABLENAME, columnsAttributes);

        FileHandle columnsFileHandle;
        if (rbfm->openFile(COLUMNS_FILENAME, columnsFileHandle) == 0)
        {
            Attribute attr;
            unsigned attrPosition = 0;
            unsigned isIndexed = 0;
            for (int i = 0; i < rids.size(); i++)
            {
                void *data = malloc(MAX_DATA_SIZE_COLUMNS_TABLE);
                if (rbfm->readRecord(columnsFileHandle, columnsAttributes, rids[i], data) == 0 && convertRecordToAttribute(columnsAttributes, data, attr, attrPosition, isIndexed) == 0)
                {
                    if (isIndexed == 1)
                    {
                        indices.push_back(attr);
                    }
                }
                free(data);
            }

            rbfm->closeFile(columnsFileHandle);
        }
    }

    return indices;
}

RC RelationManager::indexScan(const string &tableName,
                              const string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator)
{
    vector<Attribute> indices = getIndicesOfTable(tableName);

    for (int i = 0; i < indices.size(); i++)
    {
        if (indices[i].name == attributeName)
        {
            IXFileHandle ixFileHandle;
            if (ix->openFile(getIndexFileName(tableName, attributeName), ixFileHandle) == 0)
            {
                RC rc = ix->scan(ixFileHandle,
                                 indices[i],
                                 lowKey,
                                 highKey,
                                 lowKeyInclusive,
                                 highKeyInclusive,
                                 rm_IndexScanIterator.ixsi);

                return rc;
            }
            break;
        }
    }

    return -1;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)
{
    return ixsi.getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close()
{
    return ixsi.close() == 0 && ixsi.ix->closeFile(ixsi.ixfh) == 0;
}

RC RelationManager::prepareRecordForTablesTable(const unsigned &tableId, const string &tableName, const string &fileName,
                                                const SystemFlag &systemFlag, RecordManager &recordManager)
{
    recordManager.fieldValues.clear();
    recordManager.nullFlagSize = sizeof(char);
    recordManager.rawDataSize = recordManager.nullFlagSize;
    recordManager.nullFlag = (unsigned char *)malloc(recordManager.nullFlagSize);
    memset(recordManager.nullFlag, 0, recordManager.nullFlagSize);

    AttributeValuePair avPair;
    avPair.capacity = sizeof(int);
    avPair.length = avPair.capacity;
    avPair.name = TABLES_COLUMN_TABLE_ID;
    avPair.type = TypeInt;
    avPair.value = (unsigned char *)malloc(avPair.length);
    memset(avPair.value, 0, avPair.length);
    memcpy(avPair.value, &tableId, avPair.length);
    recordManager.rawDataSize += avPair.length;
    recordManager.fieldValues.push_back(avPair);

    AttributeValuePair avPair1;
    avPair1.capacity = 50;
    avPair1.length = tableName.length();
    avPair1.name = TABLES_COLUMN_TABLE_NAME;
    avPair1.type = TypeVarChar;
    avPair1.value = (unsigned char *)malloc(avPair1.length);
    memset(avPair1.value, '\0', avPair1.length);
    memcpy(avPair1.value, (char *)tableName.c_str(), avPair1.length);
    recordManager.rawDataSize += avPair1.length + sizeof(int);
    recordManager.fieldValues.push_back(avPair1);

    AttributeValuePair avPair2;
    avPair2.capacity = 50;
    avPair2.length = fileName.length();
    avPair2.name = TABLES_COLUMN_FILE_NAME;
    avPair2.type = TypeVarChar;
    avPair2.value = (unsigned char *)malloc(avPair2.length);
    memset(avPair2.value, '\0', avPair2.length);
    memcpy(avPair2.value, (char *)fileName.c_str(), avPair2.length);
    recordManager.rawDataSize += avPair2.length + sizeof(int);

    recordManager.fieldValues.push_back(avPair2);

    AttributeValuePair avPair3;
    avPair3.capacity = sizeof(int);
    avPair3.length = avPair3.capacity;
    avPair3.name = TABLES_COLUMN_SYSTEM_FLAG;
    avPair3.type = TypeInt;
    avPair3.value = (unsigned char *)malloc(avPair3.length);
    memset(avPair3.value, 0, avPair3.length);
    memcpy(avPair3.value, &systemFlag, avPair3.length);
    recordManager.rawDataSize += avPair3.length;

    recordManager.fieldValues.push_back(avPair3);

    recordManager.createData();
    return 0;
}

RC RelationManager::prepareRecordForColumnsTable(const unsigned &tableId, const string &columnName, const unsigned &columnType,
                                                 const unsigned &columnLength, const unsigned &columnPosition,
                                                 const unsigned &isIndexed, RecordManager &recordManager)
{
    recordManager.fieldValues.clear();
    recordManager.nullFlagSize = sizeof(char);
    recordManager.rawDataSize = recordManager.nullFlagSize;
    recordManager.nullFlag = (unsigned char *)malloc(recordManager.nullFlagSize);
    memset(recordManager.nullFlag, 0, recordManager.nullFlagSize);

    AttributeValuePair avPair1;
    avPair1.capacity = sizeof(int);
    avPair1.length = avPair1.capacity;
    avPair1.name = TABLES_COLUMN_TABLE_ID;
    avPair1.type = TypeInt;
    avPair1.value = (unsigned char *)malloc(avPair1.length);
    memset(avPair1.value, 0, avPair1.length);
    memcpy(avPair1.value, &tableId, avPair1.length);
    recordManager.rawDataSize += avPair1.length;

    recordManager.fieldValues.push_back(avPair1);

    AttributeValuePair avPair2;
    avPair2.capacity = 50;
    avPair2.length = columnName.length();
    avPair2.name = COLUMNS_COLUMN_COLUMN_NAME;
    avPair2.type = TypeVarChar;
    avPair2.value = (unsigned char *)malloc(avPair2.length);
    memset(avPair2.value, '\0', avPair2.length);
    memcpy(avPair2.value, (char *)columnName.c_str(), avPair2.length);
    recordManager.rawDataSize += avPair2.length + sizeof(int);

    recordManager.fieldValues.push_back(avPair2);

    AttributeValuePair avPair3;
    avPair3.capacity = sizeof(int);
    avPair3.length = avPair3.capacity;
    avPair3.name = COLUMNS_COLUMN_COLUMN_TYPE;
    avPair3.type = TypeInt;
    avPair3.value = (unsigned char *)malloc(avPair3.length);
    memset(avPair3.value, 0, avPair3.length);
    memcpy(avPair3.value, &columnType, avPair3.length);
    recordManager.rawDataSize += avPair3.length;

    recordManager.fieldValues.push_back(avPair3);

    AttributeValuePair avPair4;
    avPair4.capacity = sizeof(int);
    avPair4.length = avPair4.capacity;
    avPair4.name = COLUMNS_COLUMN_COLUMN_LENGTH;
    avPair4.type = TypeInt;
    avPair4.value = (unsigned char *)malloc(avPair4.length);
    memset(avPair4.value, 0, avPair4.length);
    memcpy(avPair4.value, &columnLength, avPair4.length);
    recordManager.rawDataSize += avPair4.length;

    recordManager.fieldValues.push_back(avPair4);

    AttributeValuePair avPair5;
    avPair5.capacity = sizeof(int);
    avPair5.length = avPair5.capacity;
    avPair5.name = COLUMNS_COLUMN_COLUMN_POSITION;
    avPair5.type = TypeInt;
    avPair5.value = (unsigned char *)malloc(avPair5.length);
    memset(avPair5.value, 0, avPair5.length);
    memcpy(avPair5.value, &columnPosition, avPair5.length);
    recordManager.rawDataSize += avPair5.length;

    recordManager.fieldValues.push_back(avPair5);

    AttributeValuePair avPair6;
    avPair6.capacity = sizeof(char);
    avPair6.length = avPair6.capacity;
    avPair6.name = COLUMNS_COLUMN_COLUMN_ISINDEXED;
    avPair6.type = TypeInt;
    avPair6.value = (unsigned char *)malloc(avPair6.length);
    memset(avPair6.value, 0, avPair6.length);
    memcpy(avPair6.value, &isIndexed, avPair6.length);
    recordManager.rawDataSize += avPair6.length;

    recordManager.fieldValues.push_back(avPair6);

    recordManager.createData();

    return 0;
}

RC RelationManager::getSchemaColumnsTable(vector<Attribute> &attributes)
{
    Attribute attr;
    attr.name = TABLES_COLUMN_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attributes.push_back(attr);

    attr.name = COLUMNS_COLUMN_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    attributes.push_back(attr);

    attr.name = COLUMNS_COLUMN_COLUMN_TYPE;
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attributes.push_back(attr);

    attr.name = COLUMNS_COLUMN_COLUMN_LENGTH;
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attributes.push_back(attr);

    attr.name = COLUMNS_COLUMN_COLUMN_POSITION;
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attributes.push_back(attr);

    attr.name = COLUMNS_COLUMN_COLUMN_ISINDEXED;
    attr.type = TypeInt;
    attr.length = (AttrLength)1;
    attributes.push_back(attr);

    return 0;
}

RC RelationManager::getSchemaTablesTable(vector<Attribute> &attributes)
{
    Attribute attr;
    attr.name = TABLES_COLUMN_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attributes.push_back(attr);

    attr.name = TABLES_COLUMN_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    attributes.push_back(attr);

    attr.name = TABLES_COLUMN_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    attributes.push_back(attr);

    attr.name = TABLES_COLUMN_SYSTEM_FLAG;
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attributes.push_back(attr);

    return 0;
}
