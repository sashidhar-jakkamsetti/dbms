
#include "qe.h"

void AggregatorStorage::update(float value)
{
    sum += value;
    count++;

    if (value > max)
    {
        max = value;
    }

    if (value < min)
    {
        min = value;
    }

    return;
}

int Utility::compare(const Attribute &attribute, const void *leftValue, const void *rightValue)
{
    if (leftValue == NULL || rightValue == NULL)
    {
        return -1;
    }

    int compareResult;
    unsigned leftValueSize = attribute.length;
    unsigned rightValueSize = attribute.length;
    if (attribute.type == TypeVarChar)
    {
        memcpy(&rightValueSize, (char *)rightValue, sizeof(unsigned));
        memcpy(&leftValueSize, (char *)leftValue, sizeof(unsigned));
        unsigned compareLen = max(leftValueSize, rightValueSize);
        char *stringleftValue = (char *)malloc(compareLen + 1);
        char *stringrightValue = (char *)malloc(compareLen + 1);
        memset(stringleftValue, '\0', compareLen + 1);
        memset(stringrightValue, '\0', compareLen + 1);
        memcpy(stringleftValue, (char *)leftValue + sizeof(unsigned), leftValueSize);
        memcpy(stringrightValue, (char *)rightValue + sizeof(unsigned), rightValueSize);

        compareResult = strcmp(stringrightValue, stringleftValue);
        free(stringleftValue);
        free(stringrightValue);
    }
    else if (attribute.type == TypeInt)
    {
        unsigned intleftValue = 0, intrightValue = 0;
        memcpy(&intleftValue, (char *)leftValue, leftValueSize);
        memcpy(&intrightValue, (char *)rightValue, rightValueSize);

        compareResult = intrightValue - intleftValue;
    }
    else
    {
        float floatleftValue = 0, floatrightValue = 0;
        memcpy(&floatleftValue, (char *)leftValue, leftValueSize);
        memcpy(&floatrightValue, (char *)rightValue, rightValueSize);

        double compareResultDouble = floatrightValue - floatleftValue;

        if (compareResultDouble == 0)
        {
            compareResult = 0;
        }
        else if (compareResultDouble > 0)
        {
            compareResult = 1;
        }
        else
        {
            compareResult = -1;
        }
    }

    return compareResult;
}

RC Utility::applyComparisionOperator(const Attribute &attr, const CompOp op, const void *leftValue, const void *rightValue)
{
    switch (op)
    {
    case NO_OP:
        return 0;
    case EQ_OP:
        if (compare(attr, leftValue, rightValue) == 0)
        {
            return 0;
        }
        break;
    case NE_OP:
        if (compare(attr, leftValue, rightValue) != 0)
        {
            return 0;
        }
        break;
    case GE_OP:
        if (compare(attr, leftValue, rightValue) >= 0)
        {
            return 0;
        }
        break;
    case GT_OP:
        if (compare(attr, leftValue, rightValue) > 0)
        {
            return 0;
        }
        break;
    case LE_OP:
        if (compare(attr, leftValue, rightValue) <= 0)
        {
            return 0;
        }
        break;
    case LT_OP:
        if (compare(attr, leftValue, rightValue) < 0)
        {
            return 0;
        }
        break;

    default:
        return -1;
        break;
    }

    return -1;
}

void Utility::createReturnData(const vector<Attribute> &attributes, RecordManager &record, void *data)
{
    unsigned nullIndicatorSize = ceil((double)attributes.size() / CHAR_BIT);
    char *nullIndicator = (char *)malloc(nullIndicatorSize);
    memset(nullIndicator, 0, nullIndicatorSize);

    unsigned pointer = nullIndicatorSize;
    int j = 0;
    for (int i = 0; i < record.fieldValues.size() && j < attributes.size(); i++)
    {
        if (attributes[j].name == record.fieldValues[i].name)
        {
            if (record.fieldValues[i].length < -1)
            {
                if (record.fieldValues[i].type == TypeVarChar)
                {
                    memcpy((char *)data + pointer, &record.fieldValues[i].length, sizeof(int));
                    pointer += sizeof(int);
                }

                memcpy((char *)data + pointer, (char *)record.fieldValues[i].value,
                       record.fieldValues[i].length);
                pointer += record.fieldValues[i].length;
            }
            else
            {
                nullIndicator[j / 8] |= (1 << (CHAR_BIT - 1 - (j % CHAR_BIT)));
            }
            j++;
        }
    }

    memcpy((char *)data, nullIndicator, nullIndicatorSize);
    return;
}

void Utility::createReturnData(const vector<Attribute> &attributes, RecordManager &leftRecord, RecordManager &rightRecord, void *data)
{
    unsigned nullIndicatorSize = ceil((double)attributes.size() / CHAR_BIT);
    char *nullIndicator = (char *)malloc(nullIndicatorSize);
    memset(nullIndicator, 0, nullIndicatorSize);

    unsigned pointer = nullIndicatorSize;
    int j = 0;
    for (int i = 0; i < leftRecord.fieldValues.size(); i++)
    {
        if (attributes[j].name == leftRecord.fieldValues[i].name)
        {
            if (leftRecord.fieldValues[i].length < -1)
            {
                if (leftRecord.fieldValues[i].type == TypeVarChar)
                {
                    memcpy((char *)data + pointer, &leftRecord.fieldValues[i].length, sizeof(int));
                    pointer += sizeof(int);
                }

                memcpy((char *)data + pointer, (char *)leftRecord.fieldValues[i].value,
                       leftRecord.fieldValues[i].length);
                pointer += leftRecord.fieldValues[i].length;
            }
            else
            {
                nullIndicator[j / 8] |= (1 << (CHAR_BIT - 1 - (j % CHAR_BIT)));
            }
            j++;
        }
    }

    for (int i = 0; i < rightRecord.fieldValues.size(); i++)
    {
        if (attributes[j].name == rightRecord.fieldValues[i].name)
        {
            if (rightRecord.fieldValues[i].length < -1)
            {
                if (rightRecord.fieldValues[i].type == TypeVarChar)
                {
                    memcpy((char *)data + pointer, &rightRecord.fieldValues[i].length, sizeof(int));
                    pointer += sizeof(int);
                }

                memcpy((char *)data + pointer, (char *)rightRecord.fieldValues[i].value,
                       rightRecord.fieldValues[i].length);
                pointer += rightRecord.fieldValues[i].length;
            }
            else
            {
                nullIndicator[j / 8] |= (1 << (CHAR_BIT - 1 - (j % CHAR_BIT)));
            }
            j++;
        }
    }

    memcpy((char *)data, nullIndicator, nullIndicatorSize);
    return;
}

Filter::Filter(Iterator *input, const Condition &condition)
{
    iterator = input;
    this->condition = condition;
    input->getAttributes(this->attributes);
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = attributes;
    return;
}

RC Filter::getNextTuple(void *data)
{
    if (!condition.bRhsIsAttr)
    {
        while (iterator->getNextTuple(data) != RBFM_EOF)
        {
            RecordManager recordManager(attributes.size());
            if (recordManager.understandData(attributes, data) == 0)
            {
                vector<AttributeValuePair>::iterator fieldValuesIter;
                for (fieldValuesIter = recordManager.fieldValues.begin();
                     fieldValuesIter != recordManager.fieldValues.end(); fieldValuesIter++)
                {
                    if (fieldValuesIter->name == condition.lhsAttr)
                    {
                        break;
                    }
                }

                if (fieldValuesIter != recordManager.fieldValues.end() && fieldValuesIter->type == condition.rhsValue.type)
                {
                    Attribute attr;
                    attr.name = fieldValuesIter->name;
                    attr.type = fieldValuesIter->type;
                    attr.length = fieldValuesIter->length;

                    if (fieldValuesIter->length < -1)
                    {
                        if (fieldValuesIter->type == TypeVarChar)
                        {
                            void *rightValue = malloc(sizeof(int) + fieldValuesIter->length);
                            memset(rightValue, 0, sizeof(int) + fieldValuesIter->length);
                            memcpy((char *)rightValue, &fieldValuesIter->length, sizeof(int));
                            memcpy((char *)rightValue + sizeof(int), (char *)fieldValuesIter->value, fieldValuesIter->length);

                            if (Utility::applyComparisionOperator(attr, condition.op, condition.rhsValue.data, rightValue) == 0)
                            {
                                recordManager.createRecord();
                                free(recordManager.record);
                                free(rightValue);
                                return 0;
                            }
                            free(rightValue);
                        }
                        else
                        {
                            if (Utility::applyComparisionOperator(attr, condition.op,
                                                                  condition.rhsValue.data, fieldValuesIter->value) == 0)
                            {
                                recordManager.createRecord();
                                free(recordManager.record);
                                return 0;
                            }
                        }
                    }
                }
                recordManager.createRecord();
                free(recordManager.record);
            }
        }
    }

    return QE_EOF;
}

Project::Project(Iterator *input, const vector<string> &attrNames)
{
    this->iterator = input;
    input->getAttributes(originalAttributes);
    rawDataMaxSize = ceil((double)originalAttributes.size() / CHAR_BIT);

    for (int i = 0; i < originalAttributes.size(); i++)
    {
        if (originalAttributes[i].type == TypeVarChar)
        {
            rawDataMaxSize += sizeof(int);
        }
        rawDataMaxSize += originalAttributes[i].length;

        if (find(attrNames.begin(), attrNames.end(), originalAttributes[i].name) != attrNames.end())
        {
            attributes.push_back(originalAttributes[i]);
        }
    }
}

RC Project::getNextTuple(void *data)
{
    void *inputData = malloc(rawDataMaxSize);
    memset(inputData, 0, rawDataMaxSize);
    while (iterator->getNextTuple(inputData) != RBFM_EOF)
    {
        RecordManager recordManager(originalAttributes.size());
        if (recordManager.understandData(originalAttributes, inputData) == 0)
        {
            Utility::createReturnData(attributes, recordManager, data);
            recordManager.createRecord();
            free(recordManager.record);
            free(inputData);
            return 0;
        }
    }

    free(inputData);
    return QE_EOF;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = attributes;
    return;
}

RC InMemoryHashTable::initialize(const Attribute &attribute, const unsigned pageMemoryLimit, Iterator *iterator,
                                 const vector<Attribute> &attributes, const unsigned rawDataMaxSize)
{
    this->attribute = attribute;
    this->pageMemoryLimit = pageMemoryLimit;
    this->iterator = iterator;
    recordCounter = 0;
    this->attributes = attributes;
    this->rawDataMaxSize = rawDataMaxSize;
    bufferMemory = 0;
}

RC Utility::getKey(const Attribute &attribute, const RecordManager &recordManager, int &key)
{
    for (int i = 0; i < recordManager.fieldValues.size(); i++)
    {
        if (attribute.name == recordManager.fieldValues[i].name && recordManager.fieldValues[i].length < -1)
        {
            memcpy(&key, (char *)recordManager.fieldValues[i].value, recordManager.fieldValues[i].length);
            return 0;
        }
    }
    return -1;
}

RC Utility::getKey(const Attribute &attribute, const RecordManager &recordManager, float &key)
{
    for (int i = 0; i < recordManager.fieldValues.size(); i++)
    {
        if (attribute.name == recordManager.fieldValues[i].name && recordManager.fieldValues[i].length < -1)
        {
            memcpy(&key, (char *)recordManager.fieldValues[i].value, recordManager.fieldValues[i].length);
            return 0;
        }
    }
    return -1;
}

RC Utility::getKey(const Attribute &attribute, const RecordManager &recordManager, string &key)
{
    for (int i = 0; i < recordManager.fieldValues.size(); i++)
    {
        if (attribute.name == recordManager.fieldValues[i].name && recordManager.fieldValues[i].length < -1)
        {
            unsigned length = recordManager.fieldValues[i].length;
            char *stringKey = (char *)malloc(length + 1);
            memset(stringKey, 0, length + 1);
            memcpy(stringKey, (char *)recordManager.fieldValues[i].value, length);
            key.append(stringKey);
            free(stringKey);
            return 0;
        }
    }
    return -1;
}

RC InMemoryHashTable::buildNextHashTable()
{
    if (recordCounter != RBFM_EOF)
    {
        bufferMemory = 0;
        while (bufferMemory <= pageMemoryLimit * PAGE_SIZE)
        {
            void *data = malloc(rawDataMaxSize);
            memset(data, 0, rawDataMaxSize);
            if (iterator->getNextTuple(data) != RBFM_EOF)
            {
                RecordManager recordManager(attributes.size());
                if (recordManager.understandData(attributes, data) == 0)
                {
                    bufferMemory += recordManager.rawDataSize;
                    recordCounter++;
                    if (attribute.type == TypeVarChar)
                    {
                        string key = "";
                        if (Utility::getKey(attribute, recordManager, key) == 0)
                        {
                            stringHashTable[key].push_back(recordManager);
                        }
                    }
                    else if (attribute.type == TypeInt)
                    {
                        int key = 0;
                        if (Utility::getKey(attribute, recordManager, key) == 0)
                        {
                            intHashTable[key].push_back(recordManager);
                        }
                    }
                    else if (attribute.type == TypeReal)
                    {
                        float key = 0.0;
                        if (Utility::getKey(attribute, recordManager, key) == 0)
                        {
                            doubleHashTable[key].push_back(recordManager);
                        }
                    }
                }
                else
                {
                    recordManager.createRecord();
                    free(recordManager.record);
                    free(data);
                    return -1;
                }
            }
            else
            {
                free(data);
                recordCounter = RBFM_EOF;
                break;
            }
        }
    }
    else
    {
        return RBFM_EOF;
    }

    return 0;
}

RC InMemoryHashTable::applyComparisionOperator(const Attribute &rightAttribute, const RecordManager &rightRecord,
                                               const CompOp op, RecordManager &leftRecord, int &atPosition)
{
    switch (op)
    {
    case EQ_OP:
        if (attribute.type == TypeVarChar)
        {
            string key = "";
            if (Utility::getKey(rightAttribute, rightRecord, key) == 0 && stringHashTable.find(key) != stringHashTable.end() && atPosition != -1)
            {
                stringHashTable[key][atPosition].copyTo(leftRecord);
                if (stringHashTable[key].size() == ++atPosition)
                {
                    atPosition = -1;
                }
                return 0;
            }
        }
        else if (attribute.type == TypeInt)
        {
            int key = 0;
            if (Utility::getKey(rightAttribute, rightRecord, key) == 0 && intHashTable.find(key) != intHashTable.end() && atPosition != -1)
            {
                intHashTable[key][atPosition].copyTo(leftRecord);
                if (intHashTable[key].size() == ++atPosition)
                {
                    atPosition = -1;
                }
                return 0;
            }
        }
        else if (attribute.type == TypeReal)
        {
            float key = 0.0;
            if (Utility::getKey(rightAttribute, rightRecord, key) == 0 && doubleHashTable.find(key) != doubleHashTable.end() && atPosition != -1)
            {
                doubleHashTable[key][atPosition].copyTo(leftRecord);
                if (doubleHashTable[key].size() == ++atPosition)
                {
                    atPosition = -1;
                }
                return 0;
            }
        }
        break;

    default:
        break;
    }

    atPosition = -1;
    return -1;
}

RC InMemoryHashTable::cleanHashTable()
{
    if (attribute.type == TypeVarChar)
    {
        map<string, vector<RecordManager>>::iterator it;
        for (it = stringHashTable.begin(); it != stringHashTable.end(); it++)
        {
            vector<RecordManager>::iterator recIt;
            for (recIt = it->second.begin(); recIt != it->second.end(); recIt++)
            {
                recIt->createRecord();
                free(recIt->record);
            }
        }
        stringHashTable.clear();
    }
    else if (attribute.type == TypeInt)
    {
        map<int, vector<RecordManager>>::iterator it;
        for (it = intHashTable.begin(); it != intHashTable.end(); it++)
        {
            vector<RecordManager>::iterator recIt;
            for (recIt = it->second.begin(); recIt != it->second.end(); recIt++)
            {
                recIt->createRecord();
                free(recIt->record);
            }
        }
        intHashTable.clear();
    }
    else if (attribute.type == TypeReal)
    {
        map<float, vector<RecordManager>, mapDoubleLess>::iterator it;
        for (it = doubleHashTable.begin(); it != doubleHashTable.end(); it++)
        {
            vector<RecordManager>::iterator recIt;
            for (recIt = it->second.begin(); recIt != it->second.end(); recIt++)
            {
                recIt->createRecord();
                free(recIt->record);
            }
        }
        doubleHashTable.clear();
    }
}

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages)
{
    leftIterator = leftIn;
    rightIterator = rightIn;
    this->condition = condition;
    pageMemoryLimit = numPages;

    leftAttributes.clear();
    leftIn->getAttributes(leftAttributes);

    rawDataMaxSizeLeft = ceil((double)leftAttributes.size() / CHAR_BIT);
    for (int i = 0; i < leftAttributes.size(); i++)
    {
        if (leftAttributes[i].name == condition.lhsAttr)
        {
            leftAttribute.name = leftAttributes[i].name;
            leftAttribute.type = leftAttributes[i].type;
            leftAttribute.length = leftAttributes[i].length;
        }

        if (leftAttributes[i].type == TypeVarChar)
        {
            rawDataMaxSizeLeft += sizeof(int);
        }
        rawDataMaxSizeLeft += leftAttributes[i].length;
    }

    rightAttributes.clear();
    rightIn->getAttributes(rightAttributes);

    rawDataMaxSizeRight = ceil((double)rightAttributes.size() / CHAR_BIT);
    for (int i = 0; i < rightAttributes.size(); i++)
    {
        if (condition.bRhsIsAttr && rightAttributes[i].name == condition.rhsAttr)
        {
            rightAttribute.name = rightAttributes[i].name;
            rightAttribute.type = rightAttributes[i].type;
            rightAttribute.length = rightAttributes[i].length;
        }

        if (rightAttributes[i].type == TypeVarChar)
        {
            rawDataMaxSizeRight += sizeof(int);
        }
        rawDataMaxSizeRight += rightAttributes[i].length;
    }

    attributes.clear();
    attributes.insert(attributes.begin(), leftAttributes.begin(), leftAttributes.end());
    attributes.insert(attributes.end(), rightAttributes.begin(), rightAttributes.end());

    inMemoryHashTable.initialize(leftAttribute, pageMemoryLimit, leftIterator, leftAttributes, rawDataMaxSizeLeft);
    leftRecordCounter = RBFM_EOF;
    rightRecordPointer = RBFM_EOF;
    rightDataBuffer = NULL;
    leftRecordPoistionInMap = 0;
}

RC BNLJoin::getNextTuple(void *data)
{
    while (
        (condition.bRhsIsAttr && leftAttribute.type == rightAttribute.type) &&
        ((rightRecordPointer != RBFM_EOF) ||
         (inMemoryHashTable.buildNextHashTable() != RBFM_EOF)))
    {
        leftRecordCounter = inMemoryHashTable.recordCounter;

        if (rightDataBuffer != NULL && leftRecordPoistionInMap != -1)
        {
            RecordManager rightRecord(rightAttributes.size());
            rightRecord.understandData(rightAttributes, rightDataBuffer);
            RecordManager leftRecord(leftAttributes.size());
            if (
                inMemoryHashTable.applyComparisionOperator(rightAttribute, rightRecord,
                                                           condition.op, leftRecord, leftRecordPoistionInMap) == 0)
            {
                Utility::createReturnData(attributes, leftRecord, rightRecord, data);
                rightRecord.createRecord();
                free(rightRecord.record);
                if (leftRecordPoistionInMap == -1)
                {
                    free(rightDataBuffer);
                    rightDataBuffer = NULL;
                }
                return 0;
            }

            rightRecord.createRecord();
            free(rightRecord.record);
        }

        while (true)
        {
            void *rightData = malloc(rawDataMaxSizeRight);
            memset(rightData, 0, rawDataMaxSizeRight);
            if (rightIterator->getNextTuple(rightData) != RBFM_EOF)
            {
                rightRecordPointer++;
                RecordManager rightRecord(rightAttributes.size());
                if (rightRecord.understandData(rightAttributes, rightData) == 0)
                {
                    RecordManager leftRecord(leftAttributes.size());
                    leftRecordPoistionInMap = 0;
                    if (
                        inMemoryHashTable.applyComparisionOperator(rightAttribute, rightRecord,
                                                                   condition.op, leftRecord, leftRecordPoistionInMap) == 0)
                    {
                        Utility::createReturnData(attributes, leftRecord, rightRecord, data);
                        rightRecord.createRecord();
                        free(rightRecord.record);
                        free(rightData);
                        if (leftRecordPoistionInMap != -1)
                        {
                            rightDataBuffer = malloc(rawDataMaxSizeRight);
                            memcpy((char *)rightDataBuffer, (char *)rightData, rawDataMaxSizeRight);
                        }
                        return 0;
                    }
                    rightRecord.createRecord();
                    free(rightRecord.record);
                }
                free(rightData);
            }
            else
            {
                rightRecordPointer = RBFM_EOF;
                rightIterator->setIterator();
                free(rightData);
                inMemoryHashTable.cleanHashTable();
                break;
            }
        }
    }
    leftRecordPoistionInMap = -1;
    rightRecordPointer = RBFM_EOF;
    return QE_EOF;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = attributes;
    return;
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition)
{
    leftIterator = leftIn;
    rightIterator = rightIn;
    this->condition = condition;

    leftAttributes.clear();
    leftIn->getAttributes(leftAttributes);

    rawDataMaxSizeLeft = ceil((double)leftAttributes.size() / CHAR_BIT);
    for (int i = 0; i < leftAttributes.size(); i++)
    {
        if (leftAttributes[i].name == condition.lhsAttr)
        {
            leftAttribute.name = leftAttributes[i].name;
            leftAttribute.type = leftAttributes[i].type;
            leftAttribute.length = leftAttributes[i].length;
        }

        if (leftAttributes[i].type == TypeVarChar)
        {
            rawDataMaxSizeLeft += sizeof(int);
        }
        rawDataMaxSizeLeft += leftAttributes[i].length;
    }

    rightAttributes.clear();
    rightIn->getAttributes(rightAttributes);

    rawDataMaxSizeRight = ceil((double)rightAttributes.size() / CHAR_BIT);
    for (int i = 0; i < rightAttributes.size(); i++)
    {
        if (condition.bRhsIsAttr && rightAttributes[i].name == condition.rhsAttr)
        {
            rightAttribute.name = rightAttributes[i].name;
            rightAttribute.type = rightAttributes[i].type;
            rightAttribute.length = rightAttributes[i].length;
        }

        if (rightAttributes[i].type == TypeVarChar)
        {
            rawDataMaxSizeRight += sizeof(int);
        }
        rawDataMaxSizeRight += rightAttributes[i].length;
    }

    attributes.clear();
    attributes.insert(attributes.begin(), leftAttributes.begin(), leftAttributes.end());
    attributes.insert(attributes.end(), rightAttributes.begin(), rightAttributes.end());

    rightRecordCounter = RBFM_EOF;
    leftDataBuffer = NULL;
    currentKey = NULL;
}

RC INLJoin::getNextTuple(void *data)
{
    while (true)
    {
        if (rightRecordCounter == RBFM_EOF)
        {
            void *leftData = malloc(rawDataMaxSizeLeft);
            memset(leftData, 0, rawDataMaxSizeLeft);
            if (leftIterator->getNextTuple(leftData) != RBFM_EOF)
            {
                RecordManager leftRecord(leftAttributes.size());
                if (leftRecord.understandData(leftAttributes, leftData) == 0)
                {
                    leftDataBuffer = malloc(rawDataMaxSizeLeft);
                    memset(leftDataBuffer, 0, rawDataMaxSizeLeft);
                    memcpy((char *)leftDataBuffer, (char *)leftData, rawDataMaxSizeLeft);

                    for (int i = 0; i < leftRecord.fieldValues.size(); i++)
                    {
                        if (leftAttribute.name == leftRecord.fieldValues[i].name && leftRecord.fieldValues[i].length < -1)
                        {
                            rightRecordCounter = 0;
                            currentKey = malloc(leftRecord.fieldValues[i].length);
                            memcpy((char *)currentKey, (char *)leftRecord.fieldValues[i].value, leftRecord.fieldValues[i].length);
                            rightIterator->setIterator(currentKey, currentKey, true, true);
                            break;
                        }
                    }

                    leftRecord.createRecord();
                    free(leftRecord.record);
                }
                free(leftData);
            }
            else
            {
                free(leftData);
                leftDataBuffer = NULL;
                break;
            }
        }

        if (rightRecordCounter != RBFM_EOF && leftDataBuffer != NULL)
        {
            void *rightData = malloc(rawDataMaxSizeRight);
            memset(rightData, 0, rawDataMaxSizeRight);
            if (rightIterator->getNextTuple(rightData) != RBFM_EOF)
            {
                rightRecordCounter++;
                RecordManager rightRecord(rightAttributes.size());
                if (rightRecord.understandData(rightAttributes, rightData) == 0)
                {
                    RecordManager leftRecord(leftAttributes.size());
                    leftRecord.understandData(leftAttributes, leftDataBuffer);

                    Utility::createReturnData(attributes, leftRecord, rightRecord, data);
                    leftRecord.createData();
                    free(leftRecord.record);
                    rightRecord.createData();
                    free(rightRecord.record);
                    free(rightData);
                    return 0;
                }
                rightRecord.createData();
                free(rightRecord.record);
            }
            else
            {
                rightRecordCounter = RBFM_EOF;
                free(currentKey);
                free(leftDataBuffer);
                leftDataBuffer = NULL;
            }

            free(rightData);
        }
    }

    return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = attributes;
    return;
}

GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPartitions)
{
    leftIterator = leftIn;
    rightIterator = rightIn;
    this->condition = condition;
    this->numPartitions = numPartitions;

    leftAttributes.clear();
    leftIn->getAttributes(leftAttributes);

    rawDataMaxSizeLeft = ceil((double)leftAttributes.size() / CHAR_BIT);
    for (int i = 0; i < leftAttributes.size(); i++)
    {
        if (leftAttributes[i].name == condition.lhsAttr)
        {
            leftAttribute.name = leftAttributes[i].name;
            leftAttribute.type = leftAttributes[i].type;
            leftAttribute.length = leftAttributes[i].length;

            leftTableName = leftAttribute.name.substr(0, leftAttribute.name.find("."));
        }

        if (leftAttributes[i].type == TypeVarChar)
        {
            rawDataMaxSizeLeft += sizeof(int);
        }
        rawDataMaxSizeLeft += leftAttributes[i].length;
    }

    rightAttributes.clear();
    rightIn->getAttributes(rightAttributes);

    rawDataMaxSizeRight = ceil((double)rightAttributes.size() / CHAR_BIT);
    for (int i = 0; i < rightAttributes.size(); i++)
    {
        if (condition.bRhsIsAttr && rightAttributes[i].name == condition.rhsAttr)
        {
            rightAttribute.name = rightAttributes[i].name;
            rightAttribute.type = rightAttributes[i].type;
            rightAttribute.length = rightAttributes[i].length;

            rightTableName = rightAttribute.name.substr(0, rightAttribute.name.find("."));
        }

        if (rightAttributes[i].type == TypeVarChar)
        {
            rawDataMaxSizeRight += sizeof(int);
        }
        rawDataMaxSizeRight += rightAttributes[i].length;
    }

    attributes.clear();
    attributes.insert(attributes.begin(), leftAttributes.begin(), leftAttributes.end());
    attributes.insert(attributes.end(), rightAttributes.begin(), rightAttributes.end());

    bnlJoinPointer = QE_EOF;
    rmLayer = RelationManager::instance();
    joinId = ghjCounter++;
    buildPhase = true;
    probePhase = false;
    currentPartition = -1;
}

inline string getPartitionFileName(const string &tableName, const unsigned &joinId, const int &partitionNumber)
{
    return tableName + "_join" + to_string(joinId) + "_" + to_string(partitionNumber) + ".part";
}

RC GHJoin::insertInPartition(const Attribute &attribute, const vector<Attribute> &attributes,
                             const void *recordData, const string &tableName)
{
    RecordManager record(attributes.size());
    if (record.understandData(attributes, recordData) == 0)
    {
        if (attribute.type == TypeVarChar)
        {
            string key = "";
            if (Utility::getKey(attribute, record, key) == 0)
            {
                int numberOfCharToConsider = ceil((double)numPartitions / 255);
                int currentKey = 0;
                for(int i = 0; i < min(numberOfCharToConsider, (int)key.length()); i++)
                {
                    currentKey += (int)key[i];
                }

                int belongsToPartition = currentKey % numPartitions;

                RID ridDummy;
                if (rmLayer->insertTuple(getPartitionFileName(tableName, joinId, belongsToPartition), recordData, ridDummy) == 0)
                {
                    record.createRecord();
                    free(record.record);
                    return 0;
                }
            }
        }
        else if (attribute.type == TypeInt)
        {
            int key = 0;
            if (Utility::getKey(attribute, record, key) == 0)
            {
                int belongsToPartition = key % numPartitions;

                RID ridDummy;
                if (rmLayer->insertTuple(getPartitionFileName(tableName, joinId, belongsToPartition), recordData, ridDummy) == 0)
                {
                    record.createRecord();
                    free(record.record);
                    return 0;
                }
            }
        }
        else if (attribute.type == TypeReal)
        {
            float key = 0.0;
            if (Utility::getKey(attribute, record, key) == 0)
            {
                int belongsToPartition = (int)floor(key) % numPartitions;

                RID ridDummy;
                if (rmLayer->insertTuple(getPartitionFileName(tableName, joinId, belongsToPartition), recordData, ridDummy) == 0)
                {
                    record.createRecord();
                    free(record.record);
                    return 0;
                }
            }
        }

        record.createRecord();
        free(record.record);
    }

    return -1;
}

RC GHJoin::getNextTuple(void *data)
{
    if (buildPhase)
    {
        probePhase = true;
        for (int i = 0; i < numPartitions; i++)
        {
            string leftPartitionFileName = getPartitionFileName(leftTableName, joinId, i);
            string rightPartitionFileName = getPartitionFileName(rightTableName, joinId, i);

            rmLayer->createTable(leftPartitionFileName, leftAttributes);
            rmLayer->createTable(rightPartitionFileName, rightAttributes);
        }

        void *leftData = malloc(rawDataMaxSizeLeft);
        memset(leftData, 0, rawDataMaxSizeLeft);
        while (leftIterator->getNextTuple(leftData) != RBFM_EOF)
        {
            if (insertInPartition(leftAttribute, leftAttributes, leftData, leftTableName) != 0)
            {
                probePhase = false;
            }
            memset(leftData, 0, rawDataMaxSizeLeft);
        }
        free(leftData);

        void *rightData = malloc(rawDataMaxSizeRight);
        memset(rightData, 0, rawDataMaxSizeRight);
        while (rightIterator->getNextTuple(rightData) != RBFM_EOF)
        {
            if (insertInPartition(rightAttribute, rightAttributes, rightData, rightTableName) != 0)
            {
                probePhase = false;
            }
            memset(rightData, 0, rawDataMaxSizeRight);
        }
        free(rightData);

        currentPartition = 0;
        buildPhase = false;
    }

    if (probePhase)
    {
        while (currentPartition > -1 && currentPartition < numPartitions)
        {
            if (bnlJoinPointer == QE_EOF)
            {
                string leftPartitionName = getPartitionFileName(leftTableName, joinId, currentPartition);
                string rightPartitionName = getPartitionFileName(rightTableName, joinId, currentPartition);

                TableScan *leftIn = new TableScan(*rmLayer, leftPartitionName);
                leftPartitionIterator = leftIn;
                TableScan *rightIn = new TableScan(*rmLayer, rightPartitionName);
                rightPartitionIterator = rightIn;

                Condition cond;
                cond.bRhsIsAttr = true;
                cond.op = condition.op;

                cond.lhsAttr = leftPartitionName + "." + leftAttribute.name;
                cond.rhsAttr = rightPartitionName + "." + rightAttribute.name;

                BNLJoin *bnlJoin = new BNLJoin(leftIn, rightIn, cond, numeric_limits<unsigned>::max());
                internalJoin = bnlJoin;
                bnlJoinPointer = 0;
            }

            if (internalJoin->getNextTuple(data) != QE_EOF)
            {
                bnlJoinPointer++;
                return 0;
            }

            delete internalJoin;
            delete leftPartitionIterator;
            delete rightPartitionIterator;
            bnlJoinPointer = QE_EOF;
            currentPartition++;
        }

        probePhase = false;
        currentPartition = -1;
    }

    return QE_EOF;
}

void GHJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = attributes;
    return;
}

GHJoin::~GHJoin()
{
    if (!buildPhase && !probePhase)
    {
        for (int i = 0; i < numPartitions; i++)
        {
            rmLayer->deleteTable(getPartitionFileName(leftTableName, joinId, i));
            rmLayer->deleteTable(getPartitionFileName(rightTableName, joinId, i));
        }
    }
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op)
{
    iterator = input;
    attribute = aggAttr;
    this->op = op;
    input->getAttributes(attributes);
    isGroupBy = false;
    nextKey = -1;
    compute = true;

    rawDataMaxSize = ceil((double)attributes.size() / CHAR_BIT);
    for (int i = 0; i < attributes.size(); i++)
    {
        if (attributes[i].type == TypeVarChar)
        {
            rawDataMaxSize += sizeof(int);
        }
        rawDataMaxSize += attributes[i].length;
    }
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr, AggregateOp op) : Aggregate(input, aggAttr, op)
{
    groupByAttribute = groupAttr;
    isGroupBy = true;
}

RC Aggregate::getNextTuple(void *data)
{
    if (compute)
    {
        void *recordData = malloc(rawDataMaxSize);
        memset(recordData, 0, rawDataMaxSize);
        while (iterator->getNextTuple(recordData) != RBFM_EOF)
        {
            RecordManager record(attributes.size());
            if (record.understandData(attributes, recordData) == 0)
            {
                for (int i = 0; i < record.fieldValues.size(); i++)
                {
                    if (attribute.name == record.fieldValues[i].name)
                    {
                        if (isGroupBy)
                        {
                            if (groupByAttribute.type == TypeVarChar)
                            {
                                string key = "";
                                if (Utility::getKey(groupByAttribute, record, key) == 0)
                                {
                                    updateAggregatorStorage(record, stringAggregatorMap[key]);
                                    stringAggIter = stringAggregatorMap.begin();
                                }
                            }
                            else if (groupByAttribute.type == TypeInt)
                            {
                                int key = 0;
                                if (Utility::getKey(groupByAttribute, record, key) == 0)
                                {
                                    updateAggregatorStorage(record, intAggregatorMap[key]);
                                    intAggIter = intAggregatorMap.begin();
                                }
                            }
                            else if (groupByAttribute.type == TypeReal)
                            {
                                float key = 0.0;
                                if (Utility::getKey(groupByAttribute, record, key) == 0)
                                {
                                    updateAggregatorStorage(record, doubleAggregatorMap[key]);
                                    doubleAggIter = doubleAggregatorMap.begin();
                                }
                            }
                        }
                        else
                        {
                            updateAggregatorStorage(record, aggStorage);
                        }
                        nextKey = 0;
                    }
                }

                record.createRecord();
                free(record.record);
            }

            memset(recordData, 0, rawDataMaxSize);
        }
        free(recordData);
        compute = false;
    }

    if (nextKey > -1)
    {
        if (isGroupBy)
        {
            if (groupByAttribute.type == TypeVarChar)
            {
                if (stringAggregatorMap.size() > nextKey && stringAggIter != stringAggregatorMap.end())
                {
                    int length = stringAggIter->first.length();
                    void *key = malloc(sizeof(int) + length);
                    memset(key, 0, sizeof(int) + length);
                    memcpy((char *)key, &length, sizeof(int));
                    memcpy((char *)key + sizeof(int), &stringAggIter->first, length);
                    createReturnData(stringAggIter->second, key, sizeof(int) + length, data);
                    stringAggIter++;
                    nextKey++;
                    free(key);
                    return 0;
                }
                else
                {
                    nextKey = -1;
                }
            }
            else if (groupByAttribute.type == TypeInt)
            {
                if (intAggregatorMap.size() > nextKey && intAggIter != intAggregatorMap.end())
                {
                    createReturnData(intAggIter->second, (int *)&intAggIter->first, sizeof(int), data);
                    intAggIter++;
                    nextKey++;
                    return 0;
                }
                else
                {
                    nextKey = -1;
                }
            }
            else if (groupByAttribute.type == TypeReal)
            {
                if (doubleAggregatorMap.size() > nextKey && doubleAggIter != doubleAggregatorMap.end())
                {
                    createReturnData(doubleAggIter->second, (float *)&doubleAggIter->first, sizeof(int), data);
                    doubleAggIter++;
                    nextKey++;
                    return 0;
                }
                else
                {
                    nextKey = -1;
                }
            }
        }
        else
        {
            createReturnData(aggStorage, NULL, 0, data);
            nextKey = -1;
            return 0;
        }
    }

    return QE_EOF;
}

void Aggregate::updateAggregatorStorage(RecordManager &record, AggregatorStorage &aggStorage)
{
    for (int i = 0; i < record.fieldValues.size(); i++)
    {
        if (attribute.name == record.fieldValues[i].name)
        {
            if (attribute.type == TypeInt)
            {
                int value = 0;
                if (Utility::getKey(attribute, record, value) == 0)
                {
                    aggStorage.update((float)value);
                }
            }
            else if (attribute.type == TypeReal)
            {
                float value = 0.0;
                if (Utility::getKey(attribute, record, value) == 0)
                {
                    aggStorage.update(value);
                }
            }
        }
    }
    return;
}

void Aggregate::createReturnData(AggregatorStorage &aggStorage, void *key, int keyLength, void *data)
{
    memset(data, 0, sizeof(char) + keyLength + sizeof(int));
    if (keyLength > 0)
    {
        memcpy((char *)data + sizeof(char), (char *)key, keyLength);
    }
    float avg = aggStorage.sum / aggStorage.count;
    switch (op)
    {
    case MAX:
        memcpy((char *)data + sizeof(char) + keyLength, &aggStorage.max, sizeof(int));
        break;
    case MIN:
        memcpy((char *)data + sizeof(char) + keyLength, &aggStorage.min, sizeof(int));
        break;
    case AVG:
        memcpy((char *)data + sizeof(char) + keyLength, &avg, sizeof(int));
        break;
    case SUM:
        memcpy((char *)data + sizeof(char) + keyLength, &aggStorage.sum, sizeof(int));
        break;
    case COUNT:
        memcpy((char *)data + sizeof(char) + keyLength, &aggStorage.count, sizeof(int));
        break;

    default:
        break;
    }

    return;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
    Attribute aggAttr;
    switch (op)
    {
    case MAX:
        aggAttr.name = "MAX(" + attribute.name + ")";
        break;
    case MIN:
        aggAttr.name = "MIN(" + attribute.name + ")";
        break;
    case AVG:
        aggAttr.name = "AVG(" + attribute.name + ")";
        break;
    case SUM:
        aggAttr.name = "SUM(" + attribute.name + ")";
        break;
    case COUNT:
        aggAttr.name = "COUNT(" + attribute.name + ")";
        break;

    default:
        break;
    }

    aggAttr.type = attribute.type;
    aggAttr.length = attribute.length;

    if (isGroupBy)
    {
        aggAttr.name = groupByAttribute.name + " " + aggAttr.name;
    }

    attrs.clear();
    attrs.push_back(aggAttr);
    return;
}
