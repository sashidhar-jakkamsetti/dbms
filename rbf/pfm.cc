#include "pfm.h"

PagedFileManager *PagedFileManager::_pf_manager = 0;

PagedFileManager *PagedFileManager::instance()
{
	if (!_pf_manager)
		_pf_manager = new PagedFileManager();

	return _pf_manager;
}

inline bool doesFileExist(const string &fileName)
{
	ifstream f(fileName.c_str());
	return f.good();
}

inline bool isFileOpen(FILE *file)
{
	return file != NULL;
}

PagedFileManager::PagedFileManager()
{
}

PagedFileManager::~PagedFileManager()
{
}

RC PagedFileManager::createFile(const string &fileName)
{
	if (!doesFileExist(fileName))
	{
		FILE *newFile;
		newFile = fopen(fileName.c_str(), "wb");

		if (newFile != NULL)
		{
			void *metaPage = malloc(PAGE_SIZE);
			memset(metaPage, 0, PAGE_SIZE);
			fseek(newFile, 0, SEEK_END);
			fwrite(metaPage, 1, PAGE_SIZE, newFile);
			fflush(newFile);
			fclose(newFile);
			free(metaPage);
			return 0;
		}
	}

	return -1;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
	return doesFileExist(fileName) ? remove(fileName.c_str()) : -1;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if (fileHandle.file != NULL)
	{
		return -1;
	}

	if (doesFileExist(fileName))
	{
		FILE *openFile;
		openFile = fopen(fileName.c_str(), "r+b");

		fileHandle.file = openFile;
		fileHandle.readPageCounter = 0;
		fileHandle.writePageCounter = 0;
		fileHandle.appendPageCounter = 0;
		return 0;
	}

	return -1;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	fileHandle.updateMetadata();
	return (fflush(fileHandle.file) == 0 && fclose(fileHandle.file) == 0) ? 0 : -1;
}

FileHandle::FileHandle()
{
	file = NULL;
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
}

FileHandle::~FileHandle()
{
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if (isFileOpen(file))
	{
		if (pageNum >= 0 && pageNum < getNumberOfPages())
		{
			fseek(file, PAGE_SIZE, SEEK_SET);
			fseek(file, pageNum * PAGE_SIZE, SEEK_CUR);
			if (fread(data, 1, PAGE_SIZE, file) == PAGE_SIZE)
			{
				readPageCounter++;
				return 0;
			}
		}
		else if (pageNum == -1)
		{
			fseek(file, 0, SEEK_SET);
			if (fread(data, PAGE_SIZE, 1, file) == 1)
				return 0;
		}
	}

	return -1;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if (isFileOpen(file))
	{
		if (pageNum >= 0 && pageNum < getNumberOfPages())
		{
			fseek(file, PAGE_SIZE, SEEK_SET);
			fseek(file, pageNum * PAGE_SIZE, SEEK_CUR);
			if (fwrite(data, 1, PAGE_SIZE, file) == PAGE_SIZE)
			{
				writePageCounter++;
				return 0;
			}
		}
		else if (pageNum == -1)
		{
			fseek(file, 0, SEEK_SET);
			if (fwrite(data, PAGE_SIZE, 1, file) == 1)
				return 0;
		}
	}

	return -1;
}

RC FileHandle::appendPage(const void *data)
{
	if (isFileOpen(file))
	{
		fseek(file, 0, SEEK_END);
		if (fwrite(data, 1, PAGE_SIZE, file) <= PAGE_SIZE)
		{
			appendPageCounter++;
			return 0;
		}
	}

	return -1;
}

unsigned FileHandle::getNumberOfPages()
{
	if (isFileOpen(file))
	{
		fseek(file, 0, SEEK_END);
		unsigned size = ftell(file);

		return size >= PAGE_SIZE ? (ceil(size / PAGE_SIZE) - 1) : 0;
	}

	return 0;
}

RC FileHandle::updateMetadata()
{
	void *metaPage = malloc(PAGE_SIZE);
	memset(metaPage, 0, PAGE_SIZE);
	int pointer = 0;
	int counter = 0;
	if (readPage(-1, metaPage) == 0)
	{
		memcpy(&counter, ((char *)metaPage + pointer), sizeof(int));
		counter += readPageCounter;
		memcpy((char *)metaPage + pointer, &counter, sizeof(int));
		pointer += sizeof(int);
		memcpy(&counter, ((char *)metaPage + pointer), sizeof(int));
		counter += writePageCounter;
		memcpy((char *)metaPage + pointer, &counter, sizeof(int));
		pointer += sizeof(int);
		memcpy(&counter, ((char *)metaPage + pointer), sizeof(int));
		counter += appendPageCounter;
		memcpy((char *)metaPage + pointer, &counter, sizeof(int));
		int error = writePage(-1, metaPage);
		free(metaPage);
		return error;
	}
	free(metaPage);
	return -1;
}

bool FileHandle::isPageFree(PageNum pageNum, const unsigned requiredSpace)
{
	void *page = malloc(PAGE_SIZE);
	memset(page, 0, PAGE_SIZE);

	if (readPage(pageNum, page) == 0)
	{
		unsigned numberOfSlots = 0, numberOfRecords = 0, freeSpace = 0;

		memcpy(&numberOfSlots, (char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE - NUMBER_OF_SLOTS_SIZE, NUMBER_OF_SLOTS_SIZE);
		memcpy(&numberOfRecords, (char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - PAGE_NUMBER_OF_RECORDS_SIZE, PAGE_NUMBER_OF_RECORDS_SIZE);
		memcpy(&freeSpace, (char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE, PAGE_FREE_SPACE_SIZE);

		freeSpace = freeSpace + (numberOfSlots - numberOfRecords) * (SLOT_OFFSET_SIZE + SLOT_LENGTH_SIZE);
		if (freeSpace >= requiredSpace)
		{
			free(page);
			return true;
		}
	}

	free(page);
	return false;
}

RC FileHandle::decrementReadCounter()
{
	readPageCounter--;
	return 0;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;

	return 0;
}
