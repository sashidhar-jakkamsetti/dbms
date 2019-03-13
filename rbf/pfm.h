#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_FREE_SPACE_SIZE 2
#define PAGE_SIZE 4096
#define NUMBER_OF_SLOTS_SIZE 2
#define PAGE_NUMBER_OF_RECORDS_SIZE 2
#define SLOT_OFFSET_SIZE 2
#define SLOT_LENGTH_SIZE 2
#include <string>
#include <climits>
#include <fstream>
#include <iostream>
#include <stdio.h>
#include <math.h>
#include <memory.h>
#include <cstring>

using namespace std;


class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                  // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                            // Create a new file
    RC destroyFile   (const string &fileName);                            // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle);    // Open a file
    RC closeFile     (FileHandle &fileHandle);                            // Close a file

protected:
    PagedFileManager();                                                   // Constructor
    ~PagedFileManager();                                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{
public:
    // variables to keep the counter for each operation
	FILE* file;
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    
    FileHandle();                                                         // Default constructor
    ~FileHandle();                                                        // Destructor

    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page
    unsigned getNumberOfPages();
    // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables
    RC updateMetadata();
    RC decrementReadCounter();

    bool isPageFree(PageNum pageNum, const unsigned requiredSpace);
}; 

#endif
