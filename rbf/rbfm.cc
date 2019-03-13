#include "rbfm.h"

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager *RecordBasedFileManager::pfm = PagedFileManager::instance();

RecordBasedFileManager *RecordBasedFileManager::instance()
{
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
	return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
	return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName,
									FileHandle &fileHandle)
{
	return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
	return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::getSlotDirectoryEntry(const unsigned &slotNum, const void *pageBuffer, unsigned &slotOffset, unsigned &slotLen)
{
	slotOffset = 0;
	slotLen = 0;

	unsigned slotOffsetStartsFrom = 0;
	slotOffsetStartsFrom = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE - NUMBER_OF_SLOTS_SIZE - ((slotNum + 1) * (SLOT_OFFSET_SIZE + SLOT_LENGTH_SIZE));

	memcpy(&slotOffset, (char *)pageBuffer + slotOffsetStartsFrom, SLOT_OFFSET_SIZE);
	memcpy(&slotLen, (char *)pageBuffer + (slotOffsetStartsFrom + SLOT_OFFSET_SIZE), SLOT_LENGTH_SIZE);

	return 0;
}

RC RecordBasedFileManager::reducePageFreeSpace(const void *pageBuffer, const unsigned &freeSpace)
{
	unsigned currentSpace;
	getPageFreeSpace(pageBuffer, currentSpace);
	currentSpace -= freeSpace;
	memcpy((char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE, &currentSpace, PAGE_FREE_SPACE_SIZE);
	return 0;
}

RC RecordBasedFileManager::increasePageFreeSpace(const void *pageBuffer, const unsigned &freeSpace)
{
	unsigned currentSpace;
	getPageFreeSpace(pageBuffer, currentSpace);
	currentSpace += freeSpace;
	memcpy((char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE, &currentSpace, PAGE_FREE_SPACE_SIZE);
	return 0;
}

RC RecordBasedFileManager::reduceNumberOfRecords(const void *pageBuffer)
{
	unsigned numberOfRecords = 0;
	getNumberOfRecords(pageBuffer, numberOfRecords);
	numberOfRecords--;
	memcpy((char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE, &numberOfRecords, PAGE_NUMBER_OF_RECORDS_SIZE);
	return 0;
}

RC RecordBasedFileManager::increaseNumberOfRecords(const void *pageBuffer)
{
	unsigned numberOfRecords = 0;
	getNumberOfRecords(pageBuffer, numberOfRecords);
	numberOfRecords++;
	memcpy((char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE, &numberOfRecords, PAGE_NUMBER_OF_RECORDS_SIZE);
	return 0;
}

RC RecordBasedFileManager::increaseNumberOfSlots(const void *pageBuffer)
{
	unsigned numberOfSlots = 0;
	getNumberOfSlots(pageBuffer, numberOfSlots);
	numberOfSlots++;
	memcpy((char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE - NUMBER_OF_SLOTS_SIZE, &numberOfSlots, NUMBER_OF_SLOTS_SIZE);
	return 0;
}

RC RecordBasedFileManager::getPageFreeSpace(const void *pageBuffer, unsigned &freeSpace)
{
	memcpy(&freeSpace, (char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE, PAGE_FREE_SPACE_SIZE);
	return 0;
}

RC RecordBasedFileManager::getNumberOfRecords(const void *pageBuffer, unsigned &numberOfRecords)
{
	memcpy(&numberOfRecords, (char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE, PAGE_NUMBER_OF_RECORDS_SIZE);
	return 0;
}

RC RecordBasedFileManager::getNumberOfSlots(const void *pageBuffer, unsigned &numberOfSlots)
{
	memcpy(&numberOfSlots, (char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE - NUMBER_OF_SLOTS_SIZE, NUMBER_OF_SLOTS_SIZE);
	return 0;
}

RC RecordBasedFileManager::updateSlotDirectory(const unsigned &slotNum, const unsigned &slotOffset, const unsigned &slotLen, void *pageBuffer)
{
	unsigned slot = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE - NUMBER_OF_SLOTS_SIZE - ((slotNum + 1) * (SLOT_OFFSET_SIZE + SLOT_LENGTH_SIZE));
	memcpy((char *)pageBuffer + slot, &slotOffset, SLOT_OFFSET_SIZE);
	memcpy((char *)pageBuffer + slot + SLOT_OFFSET_SIZE, &slotLen, SLOT_LENGTH_SIZE);
	return 0;
}

RC RecordBasedFileManager::initializePageWithMetadata(void *pageBuffer)
{
	unsigned freeSpace = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE - NUMBER_OF_SLOTS_SIZE;
	memcpy((char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE, &freeSpace, PAGE_FREE_SPACE_SIZE);

	unsigned numberOfRecords = 0;
	memcpy((char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE, &numberOfRecords, PAGE_NUMBER_OF_RECORDS_SIZE);

	unsigned numberOfSlots = 0;
	memcpy((char *)pageBuffer + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE - NUMBER_OF_SLOTS_SIZE,
		   &numberOfSlots, NUMBER_OF_SLOTS_SIZE);

	return 0;
}

RC RecordBasedFileManager::getAvailableSlot(void *pageBuffer, unsigned &availableSlotNum)
{
	unsigned nos = 0;
	getNumberOfSlots(pageBuffer, nos);

	int available = -1;
	for (unsigned slot = 0; slot < nos; slot++)
	{
		if (checkIfDeleted(pageBuffer, slot))
		{
			availableSlotNum = slot;
			available = 0;
		}
	}

	return available;
}

RC RecordBasedFileManager::getPageEndPointer(const void *page, unsigned &endPointer)
{
	unsigned nos = 0, fs = 0;
	getNumberOfSlots(page, nos);
	getPageFreeSpace(page, fs);

	endPointer = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - NUMBER_OF_SLOTS_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE - (nos * (SLOT_OFFSET_SIZE + SLOT_LENGTH_SIZE)) - fs;

	return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
	RecordManager recordManager(recordDescriptor.size());

	if (recordManager.understandData(recordDescriptor, data) == 0 && recordManager.createRecord() == 0)
	{
		unsigned recordLength = recordManager.recordSize;
		unsigned requiredSpace = recordLength + (SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE);
		int lastPageNum = (int)fileHandle.getNumberOfPages() - 1;
		int freePageNum = -1;

		if (lastPageNum >= 0)
		{
			if (fileHandle.isPageFree(lastPageNum, requiredSpace))
			{
				freePageNum = lastPageNum;
				//fileHandle.decrementReadCounter();
			}
			else
			{
				for (int i = 0; i <= lastPageNum - 1; i++)
				{
					if (fileHandle.isPageFree(i, requiredSpace))
					{
						freePageNum = i;
						//fileHandle.decrementReadCounter();
						break;
					}
				}
			}
		}

		void *page = malloc(PAGE_SIZE);
		memset(page, 0, PAGE_SIZE);
		int error = -1;

		if (freePageNum >= 0)
		{
			if (fileHandle.readPage(freePageNum, page) == 0)
			{
				unsigned numberOfRecords = 0;
				getNumberOfRecords(page, numberOfRecords);

				unsigned numberOfSlots = 0;
				getNumberOfSlots(page, numberOfSlots);

				unsigned endPointer = 0;
				getPageEndPointer(page, endPointer);
				memcpy((char *)page + endPointer, recordManager.record, recordManager.recordSize);
				increaseNumberOfRecords(page);

				unsigned availableSlotNum = 0;
				if (getAvailableSlot(page, availableSlotNum) == 0)
				{
					updateSlotDirectory(availableSlotNum, endPointer, recordManager.recordSize, page);
					reducePageFreeSpace(page, recordManager.recordSize);
					rid.slotNum = availableSlotNum;
				}
				else
				{
					updateSlotDirectory(numberOfSlots, endPointer, recordManager.recordSize, page);
					increaseNumberOfSlots(page);
					reducePageFreeSpace(page, recordManager.recordSize + (SLOT_OFFSET_SIZE + SLOT_LENGTH_SIZE));
					rid.slotNum = numberOfSlots;
				}

				fileHandle.writePage((unsigned)freePageNum, page);
				rid.pageNum = freePageNum;
				error = 0;
			}
		}
		else
		{
			initializePageWithMetadata(page);

			memcpy((char *)page, (char *)recordManager.record, recordManager.recordSize);

			updateSlotDirectory(0, 0, recordManager.recordSize, page);
			increaseNumberOfSlots(page);
			increaseNumberOfRecords(page);
			reducePageFreeSpace(page, recordManager.recordSize + (SLOT_OFFSET_SIZE + SLOT_LENGTH_SIZE));

			fileHandle.appendPage(page);
			rid.pageNum = lastPageNum + 1;
			rid.slotNum = 0;
			error = 0;
		}

		free(page);
		free(recordManager.record);
		return error;
	}

	return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
	void *page = malloc(PAGE_SIZE);
	memset(page, 0, PAGE_SIZE);
	int error = -1;

	if (rid.pageNum >= 0 && rid.slotNum >= 0 && fileHandle.readPage(rid.pageNum, page) == 0)
	{
		if (checkIfDeleted(page, rid.slotNum))
		{
			return -1;
		}

		RID updatedRid;
		if (checkIfTombStone(page, rid.slotNum, updatedRid))
		{
			return readRecord(fileHandle, recordDescriptor, updatedRid, data);
		}

		unsigned slotOffset = 0;
		unsigned slotLen = 0;
		if (getSlotDirectoryEntry(rid.slotNum, page, slotOffset, slotLen) == 0)
		{
			void *record = malloc(slotLen);
			memset(record, 0, slotLen);
			memcpy(record, (char *)page + slotOffset, slotLen);

			RecordManager recordManager(recordDescriptor.size());

			if (recordManager.understandRecord(recordDescriptor, record) == 0 && recordManager.createData() == 0)
			{
				memcpy((char *)data, (char *)recordManager.rawData, recordManager.rawDataSize);
				free(recordManager.rawData);
				error = 0;
			}
			free(record);
		}
	}

	free(page);
	return error;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
	RecordManager recordManager(recordDescriptor.size());
	if (recordManager.understandData(recordDescriptor, data) == 0)
	{
		recordManager.print();
		recordManager.createRecord();
		free(recordManager.record);
		return 0;
	}
	return -1;
}

RecordManager::RecordManager(const unsigned recordDescriptorSize)
{
	numberOfFields = recordDescriptorSize;
	record = 0;
	nullFlag = 0;
	nullFlagSize = 0;
	rawData = 0;
	recordSize = -1;
	rawDataSize = -1;
}

RC RecordManager::copyTo(RecordManager &recordManager)
{
	recordManager.numberOfFields = numberOfFields;
	recordManager.record = record;
	recordManager.nullFlag = nullFlag;
	recordManager.nullFlagSize = nullFlagSize;
	recordManager.rawData = rawData;
	recordManager.recordSize = recordSize;
	recordManager.rawDataSize = rawDataSize;
	recordManager.fieldValues = fieldValues;
}

RC RecordManager::understandData(const vector<Attribute> &recordDescriptor, const void *data)
{
	unsigned pointer = 0;
	fieldValues.clear();
	nullFlagSize = ceil((double)numberOfFields / BYTE_SIZE);
	nullFlag = (unsigned char *)malloc(nullFlagSize);
	memset(nullFlag, 0, nullFlagSize);
	memcpy(nullFlag, (char *)data + pointer, nullFlagSize);
	pointer += nullFlagSize;

	bool nullSet = false;
	recordSize = RECORD_NUMBER_OF_FIELD_SIZE + nullFlagSize + (numberOfFields * RECORD_FIELD_POINTER_SIZE);
	rawDataSize = nullFlagSize;
	for (int i = 0; i < numberOfFields; i++)
	{
		nullSet = *(nullFlag + ((int)i / BYTE_SIZE)) & (1 << (BYTE_SIZE - 1 - (i % BYTE_SIZE)));

		AttributeValuePair attrValuePair;
		attrValuePair.name = recordDescriptor[i].name;
		attrValuePair.type = recordDescriptor[i].type;
		attrValuePair.capacity = recordDescriptor[i].length;

		if (!nullSet)
		{
			if (recordDescriptor[i].type == TypeVarChar)
			{
				unsigned varCharSize = 0;
				memcpy(&varCharSize, (char *)data + pointer, sizeof(int));

				pointer += sizeof(int);
				attrValuePair.length = varCharSize;
				rawDataSize += sizeof(int);
			}
			else
			{
				attrValuePair.length = recordDescriptor[i].length;
			}

			attrValuePair.value = (unsigned char *)malloc(attrValuePair.length);
			memset(attrValuePair.value, 0, attrValuePair.length);
			memcpy(attrValuePair.value, (char *)data + pointer, attrValuePair.length);
			recordSize += attrValuePair.length;
			rawDataSize += attrValuePair.length;
			pointer += attrValuePair.length;
		}
		else
		{
			attrValuePair.length = -1;
			attrValuePair.value = 0;
		}

		fieldValues.push_back(attrValuePair);
	}

	return 0;
}

RC RecordManager::createRecord()
{
	if (recordSize > 0)
	{
		this->record = malloc(recordSize);
		memset(record, 0, recordSize);
		unsigned pointer = 0;

		memcpy((char *)this->record + pointer, &numberOfFields, RECORD_NUMBER_OF_FIELD_SIZE);
		pointer += RECORD_NUMBER_OF_FIELD_SIZE;

		memcpy((char *)this->record + pointer, nullFlag, nullFlagSize);
		free(nullFlag);
		pointer += nullFlagSize;

		unsigned actualNumberOfFields = 0;
		for (int i = 0; i < numberOfFields; i++)
		{
			if (fieldValues[i].length < -1)
			{
				actualNumberOfFields++;
			}
		}

		unsigned fieldEndPointer = RECORD_NUMBER_OF_FIELD_SIZE + nullFlagSize + (actualNumberOfFields * RECORD_FIELD_POINTER_SIZE) - 1;
		for (int i = 0; i < numberOfFields; i++)
		{
			if (fieldValues[i].length < -1)
			{
				fieldEndPointer += fieldValues[i].length;

				memcpy((char *)this->record + pointer, &fieldEndPointer,
					   RECORD_FIELD_POINTER_SIZE);
				pointer += RECORD_FIELD_POINTER_SIZE;
			}
		}

		for (int i = 0; i < numberOfFields; i++)
		{
			if (fieldValues[i].length < -1)
			{
				memcpy((char *)this->record + pointer, fieldValues[i].value, fieldValues[i].length);
				free(fieldValues[i].value);
				pointer += fieldValues[i].length;
			}
		}
		return 0;
	}
	return -1;
}

RC RecordManager::understandRecord(const vector<Attribute> &recordDescriptor, const void *record)
{
	unsigned pointer = 0;
	fieldValues.clear();

	memcpy(&numberOfFields, (char *)record + pointer, RECORD_NUMBER_OF_FIELD_SIZE);
	pointer += RECORD_NUMBER_OF_FIELD_SIZE;

	if (numberOfFields == recordDescriptor.size())
	{
		nullFlagSize = ceil((double)numberOfFields / BYTE_SIZE);
		nullFlag = (unsigned char *)malloc(nullFlagSize);
		memset(nullFlag, 0, nullFlagSize);
		memcpy(nullFlag, (char *)record + pointer, nullFlagSize);
		pointer += nullFlagSize;

		bool nullSet = false;

		unsigned actualNumberOfFields = 0;
		for (int i = 0; i < numberOfFields; i++)
		{
			nullSet = *(nullFlag + ((int)i / BYTE_SIZE)) & (1 << (BYTE_SIZE - 1 - (i % BYTE_SIZE)));

			if (!nullSet)
			{
				actualNumberOfFields++;
			}
		}

		nullSet = false;

		unsigned fieldStartPointer = RECORD_NUMBER_OF_FIELD_SIZE + nullFlagSize + (actualNumberOfFields * RECORD_FIELD_POINTER_SIZE);
		rawDataSize = nullFlagSize;

		for (int i = 0; i < numberOfFields; i++)
		{
			nullSet = *(nullFlag + ((int)i / BYTE_SIZE)) & (1 << (BYTE_SIZE - 1 - (i % BYTE_SIZE)));
			AttributeValuePair attrValuePair;
			attrValuePair.name = recordDescriptor[i].name;
			attrValuePair.type = recordDescriptor[i].type;
			attrValuePair.capacity = recordDescriptor[i].length;

			if (!nullSet)
			{
				unsigned fieldEndPointer = 0;
				memcpy(&fieldEndPointer, (char *)record + pointer, RECORD_FIELD_POINTER_SIZE);

				attrValuePair.length = fieldEndPointer - fieldStartPointer + 1;
				attrValuePair.value = (unsigned char *)malloc(attrValuePair.length);
				memset(attrValuePair.value, 0, attrValuePair.length);
				memcpy(attrValuePair.value, (byte *)record + fieldStartPointer, attrValuePair.length);
				rawDataSize += attrValuePair.length;
				if (attrValuePair.type == TypeVarChar)
				{
					rawDataSize += sizeof(int);
				}
				fieldStartPointer += attrValuePair.length;
				pointer += RECORD_FIELD_POINTER_SIZE;
			}
			else
			{
				attrValuePair.length = -1;
				attrValuePair.value = 0;
			}
			fieldValues.push_back(attrValuePair);
		}

		return 0;
	}

	return -1;
}

RC RecordManager::createData()
{
	if (rawDataSize > 0)
	{
		this->rawData = malloc(rawDataSize);
		memset(rawData, 0, rawDataSize);
		unsigned pointer = 0;
		memcpy((char *)this->rawData + pointer, nullFlag, nullFlagSize);
		free(nullFlag);
		pointer += nullFlagSize;

		for (int i = 0; i < numberOfFields; i++)
		{
			if (fieldValues[i].length < -1)
			{
				if (fieldValues[i].type == TypeVarChar)
				{
					memcpy((char *)this->rawData + pointer, &fieldValues[i].length, sizeof(int));
					pointer += sizeof(int);
				}

				memcpy((char *)this->rawData + pointer, fieldValues[i].value, fieldValues[i].length);
				free(fieldValues[i].value);
				pointer += fieldValues[i].length;
			}
		}

		return 0;
	}

	return -1;
}

RC RecordManager::print()
{
	if (!fieldValues.empty())
	{
		for (int i = 0; i < numberOfFields; i++)
		{
			if (fieldValues[i].length < -1)
			{
				switch (fieldValues[i].type)
				{
				case TypeVarChar:
				{
					unsigned char *fieldValueVarChar = (unsigned char *)malloc(fieldValues[i].length + 1);
					memset(fieldValueVarChar, 0, fieldValues[i].length + 1);
					memcpy(fieldValueVarChar, fieldValues[i].value, fieldValues[i].length);
					*(fieldValueVarChar + fieldValues[i].length) = '\0';

					printf("\t %s: %s", fieldValues[i].name.c_str(), fieldValueVarChar);
					free(fieldValueVarChar);
					break;
				}

				case TypeReal:
				{
					float fieldValueFloat;
					memcpy(&fieldValueFloat, fieldValues[i].value, fieldValues[i].length);

					printf("\t %s: %f", fieldValues[i].name.c_str(),
						   fieldValueFloat);
					break;
				}

				case TypeInt:
				{
					unsigned fieldValueInt;
					memcpy(&fieldValueInt, fieldValues[i].value, fieldValues[i].length);

					printf("\t %s: %d", fieldValues[i].name.c_str(),
						   fieldValueInt);
					break;
				}

				default:
					break;
				}
			}
			else
			{
				printf("\t %s: %s", fieldValues[i].name.c_str(), "NULL");
			}
			cout << endl;
		}
		return 0;
	}
	return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	void *page = malloc(PAGE_SIZE);
	memset(page, 0, PAGE_SIZE);
	int error = -1;

	if (rid.pageNum >= 0 && rid.slotNum >= 0 && fileHandle.readPage(rid.pageNum, page) == 0)
	{
		if (checkIfDeleted(page, rid.slotNum))
		{
			return -1;
		}

		RID updatedRid;
		if (checkIfTombStone(page, rid.slotNum, updatedRid))
		{
			return deleteRecord(fileHandle, recordDescriptor, updatedRid);
		}

		unsigned slotOffset = 0;
		unsigned slotLen = 0;
		if (getSlotDirectoryEntry(rid.slotNum, page, slotOffset, slotLen) == 0)
		{
			shiftRecordsToLeft(rid.slotNum, slotLen, page);
			reduceNumberOfRecords(page);
			increasePageFreeSpace(page, slotLen);

			fileHandle.writePage(rid.pageNum, page);
			error = 0;
		}
	}

	free(page);
	return error;
}

RC RecordBasedFileManager::shiftRecordsToLeft(const unsigned &modifyingSlotNum, const unsigned &delta, void *page)
{
	if (delta == 0)
	{
		return 0;
	}

	unsigned nos = 0, nor = 0, fs = 0;
	getNumberOfSlots(page, nos);
	getPageFreeSpace(page, fs);
	getNumberOfRecords(page, nor);

	unsigned endPointer = 0;
	getPageEndPointer(page, endPointer);

	unsigned slotOffset = 0;
	unsigned slotLen = 0;
	if (getSlotDirectoryEntry(modifyingSlotNum, page, slotOffset, slotLen) == 0)
	{
		unsigned startPointer = 0;
		startPointer = slotOffset + slotLen;
		if (startPointer <= endPointer && delta <= slotLen)
		{
			memmove((char *)page + (startPointer - delta), (char *)page + startPointer, endPointer - startPointer);

			if (updateFullSlotDirectory(modifyingSlotNum, page, delta, true) == 0)
			{
				return 0;
			}
		}
	}

	return -1;
}

RC RecordBasedFileManager::shiftRecordsToRight(const unsigned &modifyingSlotNum, const unsigned &delta, void *page)
{
	if (delta == 0)
	{
		return 0;
	}

	unsigned nos = 0, nor = 0, fs = 0;
	getNumberOfSlots(page, nos);
	getPageFreeSpace(page, fs);
	getNumberOfRecords(page, nor);

	unsigned endPointer = 0;
	getPageEndPointer(page, endPointer);

	unsigned slotOffset = 0;
	unsigned slotLen = 0;
	if (getSlotDirectoryEntry(modifyingSlotNum, page, slotOffset, slotLen) == 0)
	{
		unsigned startPointer = 0;
		startPointer = slotOffset + slotLen;
		if (startPointer <= endPointer && delta <= fs)
		{
			memmove((char *)page + (startPointer + delta), (char *)page + startPointer, endPointer - startPointer);

			if (updateFullSlotDirectory(modifyingSlotNum, page, delta, false) == 0)
			{
				return 0;
			}
		}
	}

	return -1;
}

RC RecordBasedFileManager::updateFullSlotDirectory(const unsigned &modifyingSlotNum, void *page, const unsigned &delta, const bool &negative)
{
	if (delta == 0)
	{
		return 0;
	}

	unsigned nos = 0, nor = 0, fs = 0;
	getNumberOfSlots(page, nos);
	getPageFreeSpace(page, fs);
	getNumberOfRecords(page, nor);

	unsigned currentSlotOffset = 0;
	unsigned currentSlotLen = 0;
	if (getSlotDirectoryEntry(modifyingSlotNum, page, currentSlotOffset, currentSlotLen) == 0)
	{
		unsigned slotOffset = 0;
		unsigned slotLen = 0;
		for (unsigned slot = 0; slot < nos; slot++)
		{
			if (getSlotDirectoryEntry(slot, page, slotOffset, slotLen) == 0 && !checkIfDeleted(page, slot))
			{
				if (slotOffset > currentSlotOffset)
				{
					if (negative)
					{
						updateSlotDirectory(slot, slotOffset - delta, slotLen, page);
					}
					else
					{
						updateSlotDirectory(slot, slotOffset + delta, slotLen, page);
					}
				}
				else if (slotOffset == currentSlotOffset && slot == modifyingSlotNum)
				{
					if (negative)
					{
						if (slotLen - delta == 0)
						{
							updateSlotDirectory(slot, MAXXOUT_INDICATOR, 0, page);
						}
						else
						{
							updateSlotDirectory(slot, slotOffset, slotLen - delta, page);
						}
					}
					else
					{
						updateSlotDirectory(slot, slotOffset, slotLen + delta, page);
					}
				}
			}
		}

		return 0;
	}

	return -1;
}

bool RecordBasedFileManager::checkIfDeleted(const void *pageBuffer, const unsigned &slotNum)
{
	unsigned slotOffset = 0;
	unsigned slotLen = 0;
	if (getSlotDirectoryEntry(slotNum, pageBuffer, slotOffset, slotLen) == 0)
	{
		if (slotOffset == MAXXOUT_INDICATOR)
		{
			return true;
		}
	}

	return false;
}

bool RecordBasedFileManager::checkIfTombStone(const void *pageBuffer, const unsigned &slotNum, RID &updatedRid)
{
	unsigned slotOffset = 0;
	unsigned slotLen = 0;
	if (getSlotDirectoryEntry(slotNum, pageBuffer, slotOffset, slotLen) == 0)
	{
		int numberOfFields = 0;
		memcpy(&numberOfFields, (char *)pageBuffer + slotOffset, RECORD_NUMBER_OF_FIELD_SIZE);

		if (numberOfFields == MAXXOUT_INDICATOR)
		{
			updatedRid.pageNum = 0;
			updatedRid.slotNum = 0;
			memcpy(&updatedRid.pageNum, (char *)pageBuffer + (slotOffset + RECORD_NUMBER_OF_FIELD_SIZE), PAGE_NUMBER_POINTER);
			memcpy(&updatedRid.slotNum, (char *)pageBuffer + (slotOffset + RECORD_NUMBER_OF_FIELD_SIZE + PAGE_NUMBER_POINTER), SLOT_NUMBER_POINTER);
			return true;
		}
	}

	return false;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	void *page = malloc(PAGE_SIZE);
	memset(page, 0, PAGE_SIZE);
	int error = -1;

	if (rid.pageNum >= 0 && rid.slotNum >= 0 && fileHandle.readPage(rid.pageNum, page) == 0)
	{
		if (checkIfDeleted(page, rid.slotNum))
		{
			return -1;
		}

		RID updatedRid;
		if (checkIfTombStone(page, rid.slotNum, updatedRid))
		{
			return updateRecord(fileHandle, recordDescriptor, data, updatedRid);
		}

		RecordManager recordManager(recordDescriptor.size());

		if (recordManager.understandData(recordDescriptor, data) == 0 && recordManager.createRecord() == 0)
		{
			unsigned slotOffset = 0;
			unsigned slotLen = 0;
			if (getSlotDirectoryEntry(rid.slotNum, page, slotOffset, slotLen) == 0)
			{
				if (recordManager.recordSize > slotLen)
				{
					if (fileHandle.isPageFree(rid.pageNum, recordManager.recordSize - slotLen))
					{
						shiftRecordsToRight(rid.slotNum, recordManager.recordSize - slotLen, page);
						memcpy((char *)page + slotOffset, recordManager.record, recordManager.recordSize);
						reducePageFreeSpace(page, recordManager.recordSize - slotLen);
						error = 0;
					}
					else
					{
						RID newRid;
						if (insertRecord(fileHandle, recordDescriptor, data, newRid) == 0)
						{
							void *tombStone = malloc(TOMBSTONE_SIZE);
							memset(tombStone, 0, TOMBSTONE_SIZE);
							unsigned tSIndicator = MAXXOUT_INDICATOR;
							memcpy((char *)tombStone, &tSIndicator, RECORD_NUMBER_OF_FIELD_SIZE);
							memcpy((char *)tombStone + RECORD_NUMBER_OF_FIELD_SIZE, &newRid.pageNum, PAGE_NUMBER_POINTER);
							memcpy((char *)tombStone + (RECORD_NUMBER_OF_FIELD_SIZE + PAGE_NUMBER_POINTER), &newRid.slotNum, SLOT_NUMBER_POINTER);

							if (slotLen >= TOMBSTONE_SIZE)
							{
								shiftRecordsToLeft(rid.slotNum, slotLen - TOMBSTONE_SIZE, page);
								increasePageFreeSpace(page, slotLen - TOMBSTONE_SIZE);
								memcpy((char *)page + slotOffset, tombStone, TOMBSTONE_SIZE);
								error = 0;
							}

							char *temp = (char *)malloc(6);
							memcpy(temp, (char *)tombStone, 6);

							free(tombStone);
						}
					}
				}
				else if (recordManager.recordSize < slotLen)
				{
					shiftRecordsToLeft(rid.slotNum, slotLen - recordManager.recordSize, page);
					memcpy((char *)page + slotOffset, recordManager.record, recordManager.recordSize);
					increasePageFreeSpace(page, slotLen - recordManager.recordSize);
					error = 0;
				}
				else
				{
					memcpy((char *)page + slotOffset, recordManager.record, recordManager.recordSize);
					error = 0;
				}
			}

			fileHandle.writePage(rid.pageNum, page);
			free(recordManager.record);
		}
	}

	free(page);
	return error;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
	void *page = malloc(PAGE_SIZE);
	memset(page, 0, PAGE_SIZE);

	if (rid.pageNum >= 0 && rid.slotNum >= 0 && fileHandle.readPage(rid.pageNum, page) == 0)
	{
		if (checkIfDeleted(page, rid.slotNum))
		{
			return -1;
		}

		RID updatedRid;
		if (checkIfTombStone(page, rid.slotNum, updatedRid))
		{
			return readAttribute(fileHandle, recordDescriptor, updatedRid, attributeName, data);
		}

		unsigned slotOffset = 0;
		unsigned slotLen = 0;
		if (getSlotDirectoryEntry(rid.slotNum, page, slotOffset, slotLen) == 0)
		{
			void *record = malloc(slotLen);
			memset(record, 0, slotLen);
			memcpy(record, (char *)page + slotOffset, slotLen);

			RecordManager recordManager(recordDescriptor.size());
			if (recordManager.understandRecord(recordDescriptor, record) == 0)
			{
				for (int i = 0; i < recordDescriptor.size(); i++)
				{
					if ((recordDescriptor[i].name.compare(attributeName)) == 0)
					{
						unsigned char *nullIndicator = (unsigned char *)malloc(sizeof(char));
						memset(nullIndicator, 0, sizeof(char));
						int pointer = 0;
						if (recordManager.fieldValues[i].length == -1)
						{
							nullIndicator[0] = 128; //10000000
							memcpy(data, (char *)nullIndicator, sizeof(char));
							free(record);
							free(page);
							recordManager.createData();
							free(recordManager.rawData);
							free(nullIndicator);
							return 0;
						}

						memcpy(data, (char *)nullIndicator, sizeof(char));
						pointer += sizeof(char);
						if (recordManager.fieldValues[i].type == TypeVarChar)
						{
							memcpy((char *)data + pointer, &recordManager.fieldValues[i].length, sizeof(int));
							pointer += sizeof(int);
						}

						memcpy((char *)data + pointer, recordManager.fieldValues[i].value, recordManager.fieldValues[i].length);
						free(record);
						free(page);
						free(nullIndicator);
						recordManager.createData();
						free(recordManager.rawData);
						return 0;
					}
				}
			}
			free(record);
		}
	}
	free(page);
	return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
								const vector<Attribute> &recordDescriptor,
								const string &conditionAttribute,
								const CompOp compOp,
								const void *value,
								const vector<string> &attributeNames,
								RBFM_ScanIterator &rbfm_ScanIterator)
{
	vector<int> attributes;
	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		if (find(attributeNames.begin(), attributeNames.end(), recordDescriptor[i].name) != attributeNames.end())
		{
			attributes.push_back(i);
		}
	}
	rbfm_ScanIterator.initialize(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributes);
	return 0;
}

RC RBFM_ScanIterator::initialize(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
								 const string &conditionedField,
								 const CompOp compOp, const void *value, const vector<int> &attributes)
{
	this->rbfm = RecordBasedFileManager::instance();
	this->fileHandle = fileHandle;
	this->recordDescriptor = recordDescriptor;
	this->conditionedField = conditionedField;
	this->compOp = compOp;
	this->value = (char *)value;
	this->attributes = attributes;
	this->currentRID.pageNum = 0;
	this->currentRID.slotNum = 0;
	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	unsigned numberOfPages = this->fileHandle.getNumberOfPages();
	for (unsigned i = currentRID.pageNum; i < numberOfPages; i++)
	{
		void *page = malloc(PAGE_SIZE);
		memset(page, 0, PAGE_SIZE);
		if (fileHandle.readPage(i, page) == 0)
		{
			unsigned numberOfSlots = 0;
			rbfm->getNumberOfSlots(page, numberOfSlots);
			for (unsigned j = currentRID.slotNum; j < numberOfSlots; j++)
			{
				unsigned slotOffset = 0;
				unsigned slotLen = 0;
				if (rbfm->getSlotDirectoryEntry(j, page, slotOffset, slotLen) == 0)
				{
					if (slotOffset != 65535)
					{
						void *record = malloc(slotLen);
						memset(record, 0, slotLen);
						memcpy(record, (char *)page + slotOffset, slotLen);
						unsigned numberOfFields = 0;
						memcpy(&numberOfFields, (char *)record, RECORD_NUMBER_OF_FIELD_SIZE);
						if (numberOfFields == 65535)
						{
							RID updatedRecordRID;
							unsigned pageNum = 0;
							unsigned slotNum = 0;
							int pointer = RECORD_NUMBER_OF_FIELD_SIZE;
							memcpy(&pageNum, (char *)record + pointer, PAGE_NUMBER_POINTER);
							pointer += SLOT_NUMBER_POINTER;
							memcpy(&slotNum, (char *)record + pointer, SLOT_NUMBER_POINTER);
							updatedRecordRID.pageNum = pageNum;
							updatedRecordRID.slotNum = slotNum;
							if (fileHandle.readPage(updatedRecordRID.pageNum, page) == 0)
							{
								if (rbfm->getSlotDirectoryEntry(updatedRecordRID.slotNum, page, slotOffset, slotLen) == 0)
								{
									free(record);
									record = malloc(slotLen);
									memcpy(record, (char *)page + slotOffset, slotLen);
								}
							}
						}

						RecordManager recordManager(recordDescriptor.size());

						if (recordManager.understandRecord(recordDescriptor, record) == 0)
						{
							char *stringAgainst, *stringCompare;
							unsigned valLength = 0;

							unsigned index = 0;
							bool found = false;
							for (unsigned i = 0; i < recordDescriptor.size(); i++)
							{
								if (recordDescriptor[i].name.compare(conditionedField) == 0)
								{
									index = i;
									found = true;
								}
							}

							if (compOp == NO_OP)
							{
								createData(recordManager, rid, data, i, j);
								recordManager.createData();
								free(recordManager.rawData);
								free(record);
								free(page);
								return 0;
							}
							else if (!found)
							{
								recordManager.createData();
								free(recordManager.rawData);
								free(record);
								free(page);
								return RBFM_EOF;
							}
							else if (value == NULL)
							{
								recordManager.createData();
								free(recordManager.rawData);
								free(record);
								free(page);
								return RBFM_EOF;
							}
							else if (recordManager.fieldValues[index].length == -1)
							{
								continue;
							}
							else
							{
								if (recordManager.fieldValues[index].type == TypeVarChar)
								{
									memcpy(&valLength, (char *)value, sizeof(int));
									unsigned compareValLen = recordManager.fieldValues[index].length;
									unsigned compareLen = max(valLength, compareValLen);
									stringAgainst = (char *)malloc(compareLen + 1);
									stringCompare = (char *)malloc(compareLen + 1);
									memset(stringAgainst, '\0', compareLen + 1);
									memset(stringCompare, '\0', compareLen + 1);
									memcpy(stringAgainst, (char *)value + sizeof(int), valLength);
									memcpy(stringCompare, (char *)recordManager.fieldValues[index].value, compareValLen);
								}
								else
								{
									valLength = sizeof(int);
								}

								switch (compOp)
								{
								case EQ_OP:
									if (recordManager.fieldValues[index].type == TypeVarChar)
									{
										if (strcmp((char *)stringCompare, (char *)stringAgainst) == 0)
										{
											createData(recordManager, rid, data, i, j);
											free(stringAgainst);
											free(stringCompare);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
										else
										{
											free(stringAgainst);
											free(stringCompare);
										}
									}
									else
									{
										if (memcmp(recordManager.fieldValues[index].value, (ByteValue *)value, valLength) == 0)
										{
											createData(recordManager, rid, data, i, j);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
									}

									break;

								case LT_OP:
									if (recordManager.fieldValues[index].type == TypeVarChar)
									{
										if (strcmp((char *)stringCompare, (char *)stringAgainst) < 0)
										{
											createData(recordManager, rid, data, i, j);
											free(stringAgainst);
											free(stringCompare);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
										else
										{
											free(stringAgainst);
											free(stringCompare);
										}
									}
									else
									{
										if (memcmp(recordManager.fieldValues[index].value, (ByteValue *)value, valLength) < 0)
										{
											createData(recordManager, rid, data, i, j);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
									}

									break;

								case LE_OP:
									if (recordManager.fieldValues[index].type == TypeVarChar)
									{
										if (strcmp((char *)stringCompare, (char *)stringAgainst) <= 0)
										{
											createData(recordManager, rid, data, i, j);
											free(stringAgainst);
											free(stringCompare);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
										else
										{
											free(stringAgainst);
											free(stringCompare);
										}
									}
									else
									{
										if (memcmp(recordManager.fieldValues[index].value, (ByteValue *)value, valLength) <= 0)
										{
											createData(recordManager, rid, data, i, j);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
									}

									break;

								case GT_OP:
									if (recordManager.fieldValues[index].type == TypeVarChar)
									{
										if (strcmp((char *)stringCompare, (char *)stringAgainst) > 0)
										{
											createData(recordManager, rid, data, i, j);
											free(stringAgainst);
											free(stringCompare);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
										else
										{
											free(stringAgainst);
											free(stringCompare);
										}
									}
									else
									{
										if (memcmp(recordManager.fieldValues[index].value, (ByteValue *)value, valLength) > 0)
										{
											createData(recordManager, rid, data, i, j);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
									}

									break;

								case GE_OP:
									if (recordManager.fieldValues[index].type == TypeVarChar)
									{
										if (strcmp((char *)stringCompare, (char *)stringAgainst) >= 0)
										{
											createData(recordManager, rid, data, i, j);
											free(stringAgainst);
											free(stringCompare);
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
										else
										{
											free(stringAgainst);
											free(stringCompare);
										}
									}
									else
									{
										if (memcmp(recordManager.fieldValues[index].value, (ByteValue *)value, valLength) >= 0)
										{
											createData(recordManager, rid, data, i, j);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
									}

									break;

								case NE_OP:
									if (recordManager.fieldValues[index].type == TypeVarChar)
									{
										if (strcmp((char *)stringCompare, (char *)stringAgainst) != 0)
										{
											createData(recordManager, rid, data, i, j);
											free(stringAgainst);
											free(stringCompare);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
										else
										{
											free(stringAgainst);
											free(stringCompare);
										}
									}
									else
									{
										if (memcmp(recordManager.fieldValues[index].value, (ByteValue *)value, valLength) != 0)
										{
											createData(recordManager, rid, data, i, j);
											recordManager.createData();
											free(recordManager.rawData);
											free(record);
											free(page);
											return 0;
										}
									}

									break;

								case NO_OP:
									break;
								}
							}
							recordManager.createData();
							free(recordManager.rawData);
						}
						free(record);
					}
				}
			}
		}
		free(page);
		currentRID.slotNum = 0;
	}
	return RBFM_EOF;
}

RC RBFM_ScanIterator::createData(const RecordManager recordManager, RID &rid, void *data, const int i, const int j)
{
	int nullIndicatorSize = ceil((double)attributes.size() / CHAR_BIT);
	unsigned char *nullIndicator = (unsigned char *)malloc(nullIndicatorSize);
	memset(nullIndicator, 0, nullIndicatorSize);
	int pointer = nullIndicatorSize;
	for (int k = 0; k < attributes.size(); k++)
	{
		if (recordManager.fieldValues[attributes[k]].length < -1)
		{
			if (recordManager.fieldValues[attributes[k]].type == TypeVarChar)
			{
				memcpy((char *)data + pointer, &recordManager.fieldValues[attributes[k]].length, sizeof(int));
				pointer += sizeof(int);
			}

			memcpy((char *)data + pointer, recordManager.fieldValues[attributes[k]].value, recordManager.fieldValues[attributes[k]].length);
			pointer += recordManager.fieldValues[attributes[k]].length;
		}
		else
		{
			nullIndicator[k / 8] = (1 << (BYTE_SIZE - 1 - (k % BYTE_SIZE)));
		}
	}

	memcpy((char *)data, nullIndicator, nullIndicatorSize);
	currentRID.pageNum = i;
	currentRID.slotNum = j;
	rid = currentRID;
	currentRID.slotNum++;
	free(nullIndicator);
	return 0;
}
RC RBFM_ScanIterator::close()
{
	currentRID.pageNum = -1;
	currentRID.slotNum = -1;
	return 0;
}
