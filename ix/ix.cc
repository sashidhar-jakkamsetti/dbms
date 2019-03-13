
#include "ix.h"

IndexManager *IndexManager::_index_manager = 0;
PagedFileManager *IndexManager::pfm = PagedFileManager::instance();

IndexManager *IndexManager::instance()
{
    if (!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    if (pfm->createFile(fileName) == 0)
    {
        FileHandle fileHandle;
        if (pfm->openFile(fileName, fileHandle) == 0)
        {
            void *page = malloc(PAGE_SIZE);
            memset(page, 0, PAGE_SIZE);

            if (fileHandle.readPage(-1, page) == 0)
            {
                unsigned putValue = (unsigned)NULL_INDICATOR;
                unsigned occupancy = INDEX_OCCUPANCY_D;
                memcpy((char *)page + (3 * sizeof(int)), &putValue, sizeof(int));
                memcpy((char *)page + (4 * sizeof(int)), &occupancy, sizeof(int));

                if (fileHandle.writePage(-1, page) == 0)
                {
                    free(page);
                    return pfm->closeFile(fileHandle);
                }
            }

            free(page);
        }
    }

    return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    if (pfm->openFile(fileName, ixfileHandle.fh) == 0)
    {
        return ixfileHandle.readMetadata();
    }

    return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    ixfileHandle.updateMetadata();

    return pfm->closeFile(ixfileHandle.fh);
}

RC IndexManager::getPageEndPointer(IX_Page &pageFormat, const void *page, unsigned &endPointer)
{
    if (pageFormat.isLeaf)
    {
        endPointer = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NEXT_PAGE_POINTER_SIZE - NUMBER_OF_SLOTS_SIZE - (pageFormat.numberOfSlots * (SLOT_OFFSET_SIZE + SLOT_LENGTH_SIZE)) - pageFormat.freeSpace;
    }
    else
    {
        endPointer = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NUMBER_OF_SLOTS_SIZE - (pageFormat.numberOfSlots * (SLOT_OFFSET_SIZE + SLOT_LENGTH_SIZE)) - pageFormat.freeSpace;
    }

    return 0;
}

RC IndexManager::getAffectedSlotNumForLeafPage(IX_Page &pageFormat, void *page, const Attribute &attribute,
                                               IX_LeafNode &node, unsigned &slotNum)
{
    if (pageFormat.numberOfSlots > 0)
    {
        IX_LeafNode firstNode(page, pageFormat.slotDirectory[0]);
        if (firstNode.compare(attribute, node, false) < 0)
        {
            slotNum = 0;
            free(firstNode.key);
            return 0;
        }
        free(firstNode.key);

        IX_LeafNode lastNode(page, pageFormat.slotDirectory[pageFormat.numberOfSlots - 1]);
        if (lastNode.compare(attribute, node, false) > 0)
        {
            slotNum = pageFormat.numberOfSlots;
            free(lastNode.key);
            return 0;
        }
        free(lastNode.key);

        for (int i = 0; i < pageFormat.numberOfSlots - 1; i++)
        {
            IX_LeafNode nodeI(page, pageFormat.slotDirectory[i]);
            IX_LeafNode nodeIplus1(page, pageFormat.slotDirectory[i + 1]);

            if (nodeI.compare(attribute, node, false) > 0 && nodeIplus1.compare(attribute, node, false) < 0)
            {
                slotNum = i + 1;
                free(nodeI.key);
                free(nodeIplus1.key);
                return 0;
            }
            else if (nodeI.compare(attribute, node, false) == 0)
            {
                slotNum = (unsigned)NULL_INDICATOR;
                free(nodeI.key);
                free(nodeIplus1.key);
                return 0;
            }
            free(nodeI.key);
            free(nodeIplus1.key);
        }

        return 0;
    }

    return -1;
}

RC IndexManager::getAffectedSlotNumForNonLeafPage(IX_Page &pageFormat, void *page, const Attribute &attribute,
                                                  IX_NonLeafNode &node, unsigned &slotNum)
{
    if (pageFormat.numberOfSlots > 0)
    {
        IX_NonLeafNode firstNode(page, pageFormat.slotDirectory[0]);
        if (firstNode.keyRidPair.compare(attribute, node.keyRidPair, false) < 0)
        {
            slotNum = 0;
            free(firstNode.keyRidPair.key);
            return 0;
        }
        free(firstNode.keyRidPair.key);

        IX_NonLeafNode lastNode(page, pageFormat.slotDirectory[pageFormat.numberOfSlots - 1]);
        if (lastNode.keyRidPair.compare(attribute, node.keyRidPair, false) > 0)
        {
            slotNum = pageFormat.numberOfSlots;
            free(lastNode.keyRidPair.key);
            return 0;
        }
        free(lastNode.keyRidPair.key);

        for (int i = 0; i < pageFormat.numberOfSlots - 1; i++)
        {
            IX_NonLeafNode nodeI(page, pageFormat.slotDirectory[i]);
            IX_NonLeafNode nodeIplus1(page, pageFormat.slotDirectory[i + 1]);

            if (nodeI.keyRidPair.compare(attribute, node.keyRidPair, false) > 0 && nodeIplus1.keyRidPair.compare(attribute, node.keyRidPair, false) < 0)
            {
                slotNum = i + 1;
                free(nodeI.keyRidPair.key);
                free(nodeIplus1.keyRidPair.key);
                return 0;
            }
            else if (nodeI.keyRidPair.compare(attribute, node.keyRidPair, false) == 0)
            {
                slotNum = (unsigned)NULL_INDICATOR;
                free(nodeI.keyRidPair.key);
                free(nodeIplus1.keyRidPair.key);
                return 0;
            }

            free(nodeI.keyRidPair.key);
            free(nodeIplus1.keyRidPair.key);
        }

        return 0;
    }

    return -1;
}

RC IndexManager::insertLeafNode(const Attribute &attribute, void *page, IX_LeafNode &node)
{
    if (!IsLeafPage(page))
    {
        return -1;
    }

    void *keyRid = malloc(node.getNodeSize());
    memset(keyRid, 0, node.getNodeSize());

    if (node.fabricateNode(keyRid) == 0)
    {
        IX_Page pageFormat(page);
        pageFormat.deepLoadHeader(page);

        unsigned slotNum;
        SlotEntry slotEntry;
        if (getAffectedSlotNumForLeafPage(pageFormat, page, attribute, node, slotNum) == 0)
        {
            unsigned endPointer;
            getPageEndPointer(pageFormat, page, endPointer);

            if (slotNum == (unsigned)NULL_INDICATOR)
            {
                free(keyRid);
                return 0;
            }
            else if (slotNum == pageFormat.numberOfSlots)
            {
                memcpy((char *)page + endPointer, keyRid, node.getNodeSize());

                slotEntry.slotOffset = endPointer;
                slotEntry.slotLen = node.getNodeSize();
                pageFormat.slotDirectory.push_back(slotEntry);
            }
            else
            {
                unsigned startPointer = pageFormat.slotDirectory[slotNum].slotOffset;
                if (startPointer <= endPointer)
                {
                    memmove((char *)page + (startPointer + node.getNodeSize()), (char *)page + startPointer, endPointer - startPointer);
                    memcpy((char *)page + startPointer, keyRid, node.getNodeSize());

                    slotEntry.slotOffset = startPointer;
                    slotEntry.slotLen = node.getNodeSize();

                    vector<SlotEntry>::iterator it;
                    for (it = pageFormat.slotDirectory.begin() + slotNum; it != pageFormat.slotDirectory.end(); it++)
                    {
                        it->slotOffset += slotEntry.slotLen;
                    }

                    pageFormat.slotDirectory.insert(pageFormat.slotDirectory.begin() + slotNum, slotEntry);
                }
                else
                {
                    free(keyRid);
                    return -1;
                }
            }
        }
        else
        {
            memcpy((char *)page, (char *)keyRid, node.getNodeSize());

            slotEntry.slotOffset = 0;
            slotEntry.slotLen = node.getNodeSize();
            pageFormat.slotDirectory.push_back(slotEntry);
            slotNum = 0;
        }

        //cout<<"inserting: "<<*(char *)node.key<<" "<<node.rid.pageNum<<" "<<node.rid.slotNum<<"  AT SLOT: "<<slotNum<<endl;

        pageFormat.numberOfSlots++;
        pageFormat.freeSpace -= (unsigned)(node.getNodeSize() + SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE);
        pageFormat.updateHeader(page);

        free(keyRid);
        return 0;
    }

    free(keyRid);
    return -1;
}

RC IndexManager::insertNonLeafNode(const Attribute &attribute, void *page, IX_NonLeafNode &node)
{
    if (IsLeafPage(page))
    {
        return -1;
    }

    unsigned size = node.getNodeSize();
    void *entry = malloc(node.getNodeSize());
    memset(entry, 0, node.getNodeSize());

    if (node.fabricateNode(entry) == 0)
    {
        IX_Page pageFormat(page);
        pageFormat.deepLoadHeader(page);

        unsigned slotNum;
        SlotEntry slotEntry;
        if (getAffectedSlotNumForNonLeafPage(pageFormat, page, attribute, node, slotNum) == 0)
        {
            unsigned endPointer;
            getPageEndPointer(pageFormat, page, endPointer);

            if (slotNum == (unsigned)NULL_INDICATOR)
            {
                free(entry);
                return 0;
            }
            else if (slotNum == pageFormat.numberOfSlots)
            {
                memcpy((char *)page + endPointer, entry, node.getNodeSize());

                slotEntry.slotOffset = endPointer;
                slotEntry.slotLen = node.getNodeSize();
                pageFormat.slotDirectory.push_back(slotEntry);
            }
            else
            {
                unsigned startPointer = pageFormat.slotDirectory[slotNum].slotOffset;
                if (startPointer <= endPointer)
                {
                    memmove((char *)page + (startPointer + node.getNodeSize()), (char *)page + startPointer, endPointer - startPointer);
                    memcpy((char *)page + startPointer, entry, node.getNodeSize());

                    slotEntry.slotOffset = startPointer;
                    slotEntry.slotLen = node.getNodeSize();

                    vector<SlotEntry>::iterator it;
                    for (it = pageFormat.slotDirectory.begin() + slotNum; it != pageFormat.slotDirectory.end(); it++)
                    {
                        it->slotOffset += slotEntry.slotLen;
                    }

                    pageFormat.slotDirectory.insert(pageFormat.slotDirectory.begin() + slotNum, slotEntry);
                }
                else
                {
                    free(entry);
                    return -1;
                }
            }
        }
        else
        {
            memcpy((char *)page + sizeof(PageNum), (char *)entry, node.getNodeSize());

            slotEntry.slotOffset = sizeof(PageNum);
            slotEntry.slotLen = node.getNodeSize();
            unsigned pa = 0;
            memcpy(&pa, (char *)page + slotEntry.slotOffset + slotEntry.slotLen - 4, 4);

            pageFormat.slotDirectory.push_back(slotEntry);
            slotNum = 0;
        }
        //   cout << "inserting: " << *(char *)node.keyRidPair.key <<" "<<node.keyRidPair.getNodeSize()<< " " << node.keyRidPair.rid.pageNum
        //        << " " << node.keyRidPair.rid.slotNum << " rightpage " << node.rightPage << "  AT SLOT: " << slotNum << endl;

        pageFormat.numberOfSlots++;
        pageFormat.freeSpace -= (unsigned)(node.getNodeSize() + SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE);
        pageFormat.updateHeader(page);

        free(entry);
        return 0;
    }

    free(entry);
    return -1;
}

RC IndexManager::splitNonLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute,
                                  void *page, IX_NonLeafNode &incomingNode, IX_NonLeafNode &outgoingNode)
{
    if (IsLeafPage(page))
    {
        return -1;
    }

    IX_Page pageFormat(page);
    pageFormat.deepLoadHeader(page);

    vector<IX_NonLeafNode> bagOfNodes;
    bool goneIn = false;
    for (unsigned i = 0; i < pageFormat.numberOfSlots; i++)
    {
        IX_NonLeafNode node(page, pageFormat.slotDirectory[i]);
        if (node.keyRidPair.compare(attribute, incomingNode.keyRidPair, false) <= 0 && !goneIn)
        {
            bagOfNodes.push_back(incomingNode);
            goneIn = true;
        }

        bagOfNodes.push_back(node);
    }

    if (!goneIn)
    {
        bagOfNodes.push_back(incomingNode);
    }

    PageNum oldPageLeftPagePointer = 0;
    memcpy(&oldPageLeftPagePointer, (char *)page, sizeof(PageNum));
    memset(page, 0, PAGE_SIZE);

    initializeNonLeafPage(page, oldPageLeftPagePointer);

    void *newPage = malloc(PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    PageNum newPageNum = ixfileHandle.fh.getNumberOfPages();

    bool isFirst = true;
    unsigned freeSpace = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NUMBER_OF_SLOTS_SIZE - sizeof(PageNum);
    for (unsigned i = 0; i < bagOfNodes.size(); i++)
    {
        freeSpace -= bagOfNodes[i].getNodeSize() + (SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE);
        if ((PAGE_SIZE - freeSpace) <= (unsigned)ceil(ixfileHandle.occupancy * 0.01 * PAGE_SIZE))
        {
            if (insertNonLeafNode(attribute, page, bagOfNodes[i]) != 0)
            {
                free(newPage);
                return -1;
            }
        }
        else
        {
            if (isFirst)
            {
                bagOfNodes[i].keyRidPair.copyTo(outgoingNode.keyRidPair);
                outgoingNode.rightPage = newPageNum;

                initializeNonLeafPage(newPage, bagOfNodes[i].rightPage);
                //cout<<"bifurcating node: "<<*(int *)bagOfNodes[i].keyRidPair.key<<" "<<bagOfNodes[i].keyRidPair.rid.pageNum<<endl;

                if (i == bagOfNodes.size() - 1)
                {
                    insertNonLeafNode(attribute, newPage, bagOfNodes[i]);
                }

                isFirst = false;
            }
            else
            {
                if (insertNonLeafNode(attribute, newPage, bagOfNodes[i]) != 0)
                {
                    free(newPage);
                    return -1;
                }
            }
        }
    }

    for (unsigned i = 0; i < bagOfNodes.size(); i++)
    {
        free(bagOfNodes[i].keyRidPair.key);
    }

    ixfileHandle.fh.appendPage(newPage);
    free(newPage);
    return 0;
}

RC IndexManager::splitLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute,
                               void *page, IX_LeafNode &incomingNode, IX_NonLeafNode &outgoingNode)
{
    if (!IsLeafPage(page))
    {
        return -1;
    }

    IX_Page pageFormat(page);
    pageFormat.deepLoadHeader(page);
    PageNum nextPagePointerOldPage = pageFormat.nextPage;

    vector<IX_LeafNode> bagOfNodes;
    bool goneIn = false;
    for (unsigned i = 0; i < pageFormat.numberOfSlots; i++)
    {
        IX_LeafNode node(page, pageFormat.slotDirectory[i]);
        if (node.compare(attribute, incomingNode, false) <= 0 && !goneIn)
        {
            bagOfNodes.push_back(incomingNode);
            goneIn = true;
        }

        bagOfNodes.push_back(node);
        //cout<<"loading node: "<<*(int *)node.key<<" "<<node.rid.pageNum<<endl;
    }

    if (!goneIn)
    {
        bagOfNodes.push_back(incomingNode);
    }

    memset(page, 0, PAGE_SIZE);
    initializeLeafPage(page);

    void *newPage = malloc(PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    PageNum newPageNum = ixfileHandle.fh.getNumberOfPages();

    bool isFirst = true;
    unsigned freeSpace = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NEXT_PAGE_POINTER_SIZE - NUMBER_OF_SLOTS_SIZE;
    for (unsigned i = 0; i < bagOfNodes.size(); i++)
    {
        freeSpace -= bagOfNodes[i].getNodeSize() + (SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE);
        if ((PAGE_SIZE - freeSpace) <= (unsigned)ceil(ixfileHandle.occupancy * 0.01 * PAGE_SIZE))
        {
            if (insertLeafNode(attribute, page, bagOfNodes[i]) != 0)
            {
                free(newPage);
                return -1;
            }
        }
        else
        {
            if (isFirst)
            {
                bagOfNodes[i].copyTo(outgoingNode.keyRidPair);
                outgoingNode.leftPage = (unsigned)NULL_INDICATOR;
                outgoingNode.rightPage = newPageNum;

                //cout<<"bifurcating node: "<<*(int *)bagOfNodes[i].key<<" "<<bagOfNodes[i].rid.pageNum<<endl;

                initializeLeafPage(newPage);
                insertLeafNode(attribute, newPage, outgoingNode.keyRidPair);
                isFirst = false;
            }
            else
            {
                if (insertLeafNode(attribute, newPage, bagOfNodes[i]) != 0)
                {
                    free(newPage);
                    return -1;
                }
            }
        }
    }

    for (unsigned i = 0; i < bagOfNodes.size(); i++)
    {
        free(bagOfNodes[i].key);
    }

    setNextPagePointer(page, newPageNum);
    setNextPagePointer(newPage, nextPagePointerOldPage);

    ixfileHandle.fh.appendPage(newPage);
    free(newPage);
    return 0;
}

RC IndexManager::setNextPagePointer(void *page, PageNum &nextPageNum)
{
    if (IsLeafPage(page))
    {
        memcpy((char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NEXT_PAGE_POINTER_SIZE,
               &nextPageNum, NEXT_PAGE_POINTER_SIZE);
    }

    return 0;
}

bool IndexManager::IsLeafPage(const void *page)
{
    unsigned indicator = 0;
    memcpy(&indicator, (char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE,
           LEAF_NODE_INDICATOR_SIZE);
    return indicator == 1;
}

RC IndexManager::initializeLeafPage(void *page)
{
    unsigned freeSpace = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - NEXT_PAGE_POINTER_SIZE - LEAF_NODE_INDICATOR_SIZE - NUMBER_OF_SLOTS_SIZE;
    memcpy((char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE, &freeSpace, PAGE_FREE_SPACE_SIZE);

    unsigned leafIndicator = 1;
    memcpy((char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE,
           &leafIndicator, LEAF_NODE_INDICATOR_SIZE);

    unsigned nextPagePointer = (unsigned)NULL_INDICATOR;
    memcpy((char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NEXT_PAGE_POINTER_SIZE,
           &nextPagePointer, NEXT_PAGE_POINTER_SIZE);

    unsigned numberOfSlots = 0;
    memcpy((char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NEXT_PAGE_POINTER_SIZE - NUMBER_OF_SLOTS_SIZE,
           &numberOfSlots, NUMBER_OF_SLOTS_SIZE);

    return 0;
}

RC IndexManager::initializeNonLeafPage(void *page, PageNum defaultLeftPagePointer)
{
    unsigned freeSpace = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NUMBER_OF_SLOTS_SIZE - sizeof(PageNum);
    memcpy((char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE, &freeSpace, PAGE_FREE_SPACE_SIZE);

    unsigned leafIndicator = 0;
    memcpy((char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE,
           &leafIndicator, LEAF_NODE_INDICATOR_SIZE);

    unsigned numberOfSlots = 0;
    memcpy((char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NUMBER_OF_SLOTS_SIZE,
           &numberOfSlots, NUMBER_OF_SLOTS_SIZE);

    memcpy((char *)page, &defaultLeftPagePointer, sizeof(PageNum));

    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    PageNum rootPageNum;

    if (ixfileHandle.root == (unsigned)NULL_INDICATOR && ixfileHandle.fh.getNumberOfPages() == 0)
    {
        ixfileHandle.root = 0;
        initializeLeafPage(page);
        //ixfileHandle.fh.appendPage(page);
    }
    else 
    {
        if (getRootPage(ixfileHandle, page) != 0)
        {
            return -1;
        }
    }

    rootPageNum = ixfileHandle.root;

    IX_NonLeafNode newChild;
    if (insertRoutine(ixfileHandle, attribute, key, rid, page, rootPageNum, newChild) == 0)
    {
        if (newChild.rightPage != (unsigned)NULL_INDICATOR)
        {
            void *newPage = malloc(PAGE_SIZE);
            memset(newPage, 0, PAGE_SIZE);

            initializeNonLeafPage(newPage, rootPageNum);
            PageNum newPageNum = ixfileHandle.fh.getNumberOfPages();

            if (insertNonLeafNode(attribute, newPage, newChild) == 0)
            {
                ixfileHandle.fh.appendPage(newPage);
                ixfileHandle.root = newPageNum;
                ixfileHandle.updateMetadata();
            }

            free(newPage);
            
        }

        if(newChild.leftPage == (unsigned)NULL_INDICATOR)
        {
            //free(newChild.keyRidPair.key);
        }
        free(page);
        return 0;
    }

    free(page);
    return -1;
}

RC IndexManager::insertRoutine(IXFileHandle &ixfileHandle, const Attribute &attribute,
                               const void *key, const RID &rid, void *page, const PageNum &pageNum, IX_NonLeafNode &newChild)
{
    if (IsLeafPage(page))
    {
        IX_Page pageFormat(page);
        IX_LeafNode node(attribute, key, rid);
        unsigned requiredSpace = node.getNodeSize() + (SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE);

        if (pageFormat.freeSpace > requiredSpace)
        {
            if (insertLeafNode(attribute, page, node) == 0)
            {
                newChild.rightPage = (unsigned)NULL_INDICATOR;
                free(node.key);
                if(pageNum == ixfileHandle.fh.getNumberOfPages() && ixfileHandle.fh.getNumberOfPages() == 0)
                {
                    return ixfileHandle.fh.appendPage(page);
                }
                else
                {
                    return ixfileHandle.fh.writePage(pageNum, page);
                }
            }
        }
        else
        {
            if (splitLeafPage(ixfileHandle, attribute, page, node, newChild) == 0)
            {
                //free(node.key);
                return ixfileHandle.fh.writePage(pageNum, page);
            }
        }
    }
    else
    {
        PageNum newPageNum = 0;
        if (findPage(attribute, key, rid, page, newPageNum) == 0)
        {
            void *newPage = malloc(PAGE_SIZE);
            memset(newPage, 0, PAGE_SIZE);
            if (ixfileHandle.fh.readPage(newPageNum, newPage) != 0)
            {
                return -1;
            }

            if (insertRoutine(ixfileHandle, attribute, key, rid, newPage, newPageNum, newChild) == 0)
            {
                if (newChild.rightPage == (unsigned)NULL_INDICATOR)
                {
                    free(newPage);
                    return 0;
                }
                else
                {
                    IX_Page pageFormat(page);

                    unsigned requiredSpace = newChild.getNodeSize() + (SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE);
                    if (pageFormat.freeSpace >= requiredSpace)
                    {
                        if (insertNonLeafNode(attribute, page, newChild) == 0)
                        {
                            newChild.rightPage = (unsigned)NULL_INDICATOR;
                            if (ixfileHandle.fh.writePage(pageNum, page) == 0)
                            {
                                free(newPage);
                                return 0;
                            }
                        }
                    }
                    else
                    {
                        IX_NonLeafNode outChild;
                        if (splitNonLeafPage(ixfileHandle, attribute, page, newChild, outChild) == 0)
                        {
                            outChild.copyTo(newChild);
                            if (ixfileHandle.fh.writePage(pageNum, page) == 0)
                            {
                                free(outChild.keyRidPair.key);
                                free(newPage);
                                return 0;
                            }
                        }
                    }
                }
            }

            free(newPage);
        }
    }

    return -1;
}

RC IndexManager::deletedLeafNode(const Attribute &attribute, void *page, IX_LeafNode &node)
{
    if (!IsLeafPage(page))
    {
        return -1;
    }

    IX_Page pageFormat(page);
    pageFormat.deepLoadHeader(page);

    if (pageFormat.numberOfSlots > 0)
    {
        unsigned slotToDelete = (unsigned)NULL_INDICATOR;
        for (unsigned i = 0; i < pageFormat.numberOfSlots; i++)
        {
            IX_LeafNode currentNode(page, pageFormat.slotDirectory[i]);
            if (currentNode.compare(attribute, node, false) == 0)
            {
                slotToDelete = i;
                free(currentNode.key);
                break;
            }
            free(currentNode.key);
        }

        if (slotToDelete != (unsigned)NULL_INDICATOR)
        {
            unsigned endPointer;
            getPageEndPointer(pageFormat, page, endPointer);

            unsigned startPointer = pageFormat.slotDirectory[slotToDelete].slotOffset + pageFormat.slotDirectory[slotToDelete].slotLen;
            if (startPointer <= endPointer)
            {
                memmove((char *)page + startPointer - pageFormat.slotDirectory[slotToDelete].slotLen,
                        (char *)page + startPointer, endPointer - startPointer);

                vector<SlotEntry>::iterator it;
                for (it = pageFormat.slotDirectory.begin() + slotToDelete + 1; it != pageFormat.slotDirectory.end(); it++)
                {
                    it->slotOffset -= pageFormat.slotDirectory[slotToDelete].slotLen;
                }

                pageFormat.slotDirectory.erase(pageFormat.slotDirectory.begin() + slotToDelete);
                pageFormat.numberOfSlots--;
                pageFormat.freeSpace += node.getNodeSize() + SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE;
                pageFormat.updateHeader(page);

                //cout<<"deleting: "<<*(float *)node.key<<" "<<node.rid.pageNum<<" "<<node.rid.slotNum<<"  AT SLOT: "<<slotToDelete<<endl;

                return 0;
            }
        }
    }

    return -1;
}

RC IndexManager::deletedNonLeafNode(const Attribute &attribute, void *page, IX_NonLeafNode &node)
{
    if (IsLeafPage(page))
    {
        return -1;
    }

    IX_Page pageFormat(page);
    pageFormat.deepLoadHeader(page);

    if (pageFormat.numberOfSlots > 0)
    {
        unsigned slotToDelete = (unsigned)NULL_INDICATOR;
        for (unsigned i = 0; i < pageFormat.numberOfSlots; i++)
        {
            IX_NonLeafNode currentNode(page, pageFormat.slotDirectory[i]);
            if (currentNode.keyRidPair.compare(attribute, node.keyRidPair, false) == 0)
            {
                slotToDelete = i;
                free(currentNode.keyRidPair.key);
                break;
            }
            free(currentNode.keyRidPair.key);
        }

        if (slotToDelete != (unsigned)NULL_INDICATOR)
        {
            unsigned endPointer;
            getPageEndPointer(pageFormat, page, endPointer);

            unsigned startPointer = pageFormat.slotDirectory[slotToDelete].slotOffset + pageFormat.slotDirectory[slotToDelete].slotLen;
            if (startPointer <= endPointer)
            {
                memmove((char *)page + startPointer - pageFormat.slotDirectory[slotToDelete].slotLen,
                        (char *)page + startPointer, endPointer - startPointer);

                vector<SlotEntry>::iterator it;
                for (it = pageFormat.slotDirectory.begin() + slotToDelete + 1; it != pageFormat.slotDirectory.end(); it++)
                {
                    it->slotOffset -= pageFormat.slotDirectory[slotToDelete].slotLen;
                }

                pageFormat.slotDirectory.erase(pageFormat.slotDirectory.begin() + slotToDelete);
                pageFormat.numberOfSlots--;
                pageFormat.freeSpace += node.getNodeSize() + SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE;
                pageFormat.updateHeader(page);

                //  cout << "deleting: " << *(char *)node.keyRidPair.key <<" "<<node.keyRidPair.getNodeSize()<< " " << node.keyRidPair.rid.pageNum
                //       << " " << node.keyRidPair.rid.slotNum << " rightpage " << node.rightPage << "  AT SLOT: " << slotToDelete << endl;
                return 0;
            }
        }
    }

    return -1;
}

RC IndexManager::redistributeLeafPage(const Attribute &attribute, const unsigned &occupancyThreshold, void *page,
                                      void *siblingPage, const PageNum &siblingPageNum, void *parentPage)
{
    if (!IsLeafPage(page) || !IsLeafPage(siblingPage))
    {
        return -1;
    }

    IX_Page parentPageFormat(parentPage);
    parentPageFormat.deepLoadHeader(parentPage);

    unsigned parentPageReplaceSlotNum = (unsigned)NULL_INDICATOR;
    for (unsigned i = 0; i < parentPageFormat.numberOfSlots; i++)
    {
        IX_NonLeafNode currentNode(parentPage, parentPageFormat.slotDirectory[i]);
        if (currentNode.rightPage == siblingPageNum)
        {
            parentPageReplaceSlotNum = i;
            free(currentNode.keyRidPair.key);
            break;
        }
        free(currentNode.keyRidPair.key);
    }

    if (parentPageReplaceSlotNum == (unsigned)NULL_INDICATOR)
    {
        return -1;
    }

    unsigned totalSIze = 0;
    vector<IX_LeafNode> bagOfNodes;
    IX_Page pageFormat(page);
    pageFormat.deepLoadHeader(page);
    for (unsigned i = 0; i < pageFormat.numberOfSlots; i++)
    {
        IX_LeafNode currentNode(page, pageFormat.slotDirectory[i]);
        bagOfNodes.push_back(currentNode);
        totalSIze += currentNode.getNodeSize();
    }

    IX_Page siblingPageFormat(siblingPage);
    siblingPageFormat.deepLoadHeader(siblingPage);
    for (unsigned i = 0; i < siblingPageFormat.numberOfSlots; i++)
    {
        IX_LeafNode currentNode(siblingPage, siblingPageFormat.slotDirectory[i]);
        bagOfNodes.push_back(currentNode);
        totalSIze += currentNode.getNodeSize();
    }

    if (totalSIze >= (occupancyThreshold + PAGE_SIZE - (18 + siblingPageFormat.numberOfSlots * (SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE))))
    {
        return 0;
    }

    memset(page, 0, PAGE_SIZE);
    initializeLeafPage(page);
    setNextPagePointer(page, pageFormat.nextPage);

    IX_NonLeafNode nodeToReplace;
    bool isFirst = true;
    unsigned freeSpace = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NEXT_PAGE_POINTER_SIZE - NUMBER_OF_SLOTS_SIZE;
    for (unsigned i = 0; i < bagOfNodes.size(); i++)
    {
        if ((PAGE_SIZE - freeSpace) <= occupancyThreshold)
        {
            if (insertLeafNode(attribute, page, bagOfNodes[i]) != 0)
            {
                return -1;
            }

            freeSpace -= bagOfNodes[i].getNodeSize() + (SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE);
        }
        else
        {
            if (isFirst)
            {
                bagOfNodes[i].copyTo(nodeToReplace.keyRidPair);
                nodeToReplace.rightPage = siblingPageNum;

                memset(siblingPage, 0, PAGE_SIZE);
                initializeLeafPage(siblingPage);
                setNextPagePointer(siblingPage, siblingPageFormat.nextPage);
                isFirst = false;
            }

            if (insertLeafNode(attribute, siblingPage, bagOfNodes[i]) != 0)
            {
                return -1;
            }
        }
    }

    for (unsigned i = 0; i < bagOfNodes.size(); i++)
    {
        free(bagOfNodes[i].key);
    }

    if (!isFirst && parentPageReplaceSlotNum != (unsigned)NULL_INDICATOR)
    {
        // void *entry = malloc(nodeToReplace.getNodeSize());
        // nodeToReplace.fabricateNode(entry);

        IX_NonLeafNode deleteParentNode(parentPage, parentPageFormat.slotDirectory[parentPageReplaceSlotNum]);
        if (labs(nodeToReplace.getNodeSize() - deleteParentNode.getNodeSize()) > parentPageFormat.freeSpace)
        {
            free(deleteParentNode.keyRidPair.key);
            return -2;
        }
        if (deletedNonLeafNode(attribute, parentPage, deleteParentNode) == 0 && insertNonLeafNode(attribute, parentPage, nodeToReplace) == 0)
        {
            free(deleteParentNode.keyRidPair.key);
            return 0;
        }
        free(deleteParentNode.keyRidPair.key);

        // // TODO: Write this
        // if (parentPageFormat.updateNode(attribute, nodeToReplace, parentPageReplaceSlotNum) == 0)
        // {
        //     return 0;
        // }
    }

    return -1;
}

RC IndexManager::redistributeNonLeafPage(const Attribute &attribute, const unsigned &occupancyThreshold, void *page,
                                         void *siblingPage, const PageNum &siblingPageNum, void *parentPage)
{
    if (IsLeafPage(page) || IsLeafPage(siblingPage))
    {
        return -1;
    }

    IX_Page parentPageFormat(parentPage);
    parentPageFormat.deepLoadHeader(parentPage);

    unsigned parentPageReplaceSlotNum = (unsigned)NULL_INDICATOR;
    IX_NonLeafNode parentReplaceNode;
    for (unsigned i = 0; i < parentPageFormat.numberOfSlots; i++)
    {
        IX_NonLeafNode currentNode(parentPage, parentPageFormat.slotDirectory[i]);
        if (currentNode.rightPage == siblingPageNum)
        {
            parentPageReplaceSlotNum = i;
            currentNode.copyTo(parentReplaceNode);
            memcpy(&parentReplaceNode.rightPage, (char *)siblingPage, sizeof(PageNum));
            free(currentNode.keyRidPair.key);
            break;
        }
        free(currentNode.keyRidPair.key);
    }

    if (parentPageReplaceSlotNum == (unsigned)NULL_INDICATOR)
    {
        return -1;
    }

    unsigned totalSIze = 0;
    vector<IX_NonLeafNode> bagOfNodes;
    IX_Page pageFormat(page);
    pageFormat.deepLoadHeader(page);

    IX_Page siblingPageFormat(siblingPage);
    siblingPageFormat.deepLoadHeader(siblingPage);

    for (unsigned i = 0; i < pageFormat.numberOfSlots; i++)
    {
        IX_NonLeafNode currentNode(page, pageFormat.slotDirectory[i]);
        bagOfNodes.push_back(currentNode);
        totalSIze += currentNode.getNodeSize();
    }
    bagOfNodes.push_back(parentReplaceNode);
    for (unsigned i = 0; i < siblingPageFormat.numberOfSlots; i++)
    {
        IX_NonLeafNode currentNode(siblingPage, siblingPageFormat.slotDirectory[i]);
        bagOfNodes.push_back(currentNode);
        totalSIze += currentNode.getNodeSize();
    }

    if (totalSIze >= (occupancyThreshold + PAGE_SIZE - (18 + siblingPageFormat.numberOfSlots * (SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE))))
    {
        return 0;
    }

    PageNum leftPagePointer = 0;
    memcpy(&leftPagePointer, (char *)page, sizeof(PageNum));
    memset(page, 0, PAGE_SIZE);
    initializeNonLeafPage(page, leftPagePointer);

    IX_NonLeafNode nodeToReplaceParent;
    bool isFirst = true;
    unsigned freeSpace = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NUMBER_OF_SLOTS_SIZE - sizeof(PageNum);
    for (unsigned i = 0; i < bagOfNodes.size(); i++)
    {
        if ((PAGE_SIZE - freeSpace) <= occupancyThreshold)
        {
            if (insertNonLeafNode(attribute, page, bagOfNodes[i]) != 0)
            {
                return -1;
            }

            freeSpace -= bagOfNodes[i].getNodeSize() + (SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE);
        }
        else
        {
            if (isFirst)
            {
                bagOfNodes[i].copyTo(nodeToReplaceParent);
                IX_NonLeafNode parentReplaceSlotNode(parentPage, parentPageFormat.slotDirectory[parentPageReplaceSlotNum]);
                nodeToReplaceParent.rightPage = parentReplaceSlotNode.rightPage;

                memset(siblingPage, 0, PAGE_SIZE);
                initializeNonLeafPage(siblingPage, bagOfNodes[i].rightPage);
                isFirst = false;
            }
            else
            {
                if (insertNonLeafNode(attribute, siblingPage, bagOfNodes[i]) != 0)
                {
                    return -1;
                }
            }
        }
    }

    for (unsigned i = 0; i < bagOfNodes.size(); i++)
    {
        free(bagOfNodes[i].keyRidPair.key);
    }

    if (!isFirst && parentPageReplaceSlotNum != (unsigned)NULL_INDICATOR)
    {
        // void *entry = malloc(nodeToReplaceParent.getNodeSize());
        // nodeToReplaceParent.fabricateNode(entry);

        IX_NonLeafNode deleteParentNode(parentPage, parentPageFormat.slotDirectory[parentPageReplaceSlotNum]);
        if (labs(nodeToReplaceParent.getNodeSize() - deleteParentNode.getNodeSize()) > parentPageFormat.freeSpace)
        {
            free(deleteParentNode.keyRidPair.key);
            return -2;
        }

        if (deletedNonLeafNode(attribute, parentPage, deleteParentNode) == 0 && insertNonLeafNode(attribute, parentPage, nodeToReplaceParent) == 0)
        {
            free(deleteParentNode.keyRidPair.key);
            return 0;
        }
        free(deleteParentNode.keyRidPair.key);

        // // TODO: Write this
        // if (parentPageFormat.updateNode(attribute, nodeToReplaceParent, parentPageReplaceSlotNum) == 0)
        // {
        //     return 0;
        // }
    }

    return -1;
}

RC IndexManager::mergeLeafNode(const Attribute &attribute, void *page, const PageNum &siblingPageNum,
                               void *siblingPage, void *parentPage, IX_NonLeafNode &oldChild)
{
    if (!IsLeafPage(page) || !IsLeafPage(siblingPage))
    {
        return -1;
    }

    IX_Page parentPageFormat(parentPage);
    parentPageFormat.deepLoadHeader(parentPage);

    unsigned parentPageReplaceSlotNum = (unsigned)NULL_INDICATOR;
    for (unsigned i = 0; i < parentPageFormat.numberOfSlots; i++)
    {
        IX_NonLeafNode currentNode(parentPage, parentPageFormat.slotDirectory[i]);
        if (currentNode.rightPage == siblingPageNum)
        {
            currentNode.copyTo(oldChild);
            parentPageReplaceSlotNum = i;
            free(currentNode.keyRidPair.key);
            break;
        }
        free(currentNode.keyRidPair.key);
    }

    if (parentPageReplaceSlotNum == (unsigned)NULL_INDICATOR)
    {
        return -1;
    }

    IX_Page siblingPageForamt(siblingPage);
    siblingPageForamt.deepLoadHeader(siblingPage);

    for (unsigned i = 0; i < siblingPageForamt.numberOfSlots; i++)
    {
        IX_LeafNode currentNode(siblingPage, siblingPageForamt.slotDirectory[i]);
        if (insertLeafNode(attribute, page, currentNode) != 0)
        {
            free(currentNode.key);
            return -1;
        }
        free(currentNode.key);
    }

    setNextPagePointer(page, siblingPageForamt.nextPage);
    memset(siblingPage, 0, PAGE_SIZE);
    // Kill right page.

    return 0;
}

RC IndexManager::mergeNonLeafNode(const Attribute &attribute, void *page, const PageNum &siblingPageNum,
                                  void *siblingPage, void *parentPage, IX_NonLeafNode &oldChild)
{
    if (IsLeafPage(page) || IsLeafPage(siblingPage))
    {
        return -1;
    }

    IX_Page parentPageFormat(parentPage);
    parentPageFormat.deepLoadHeader(parentPage);

    unsigned parentPageReplaceSlotNum = (unsigned)NULL_INDICATOR;
    IX_NonLeafNode parentReplaceNode;
    for (unsigned i = 0; i < parentPageFormat.numberOfSlots; i++)
    {
        IX_NonLeafNode currentNode(parentPage, parentPageFormat.slotDirectory[i]);
        if (currentNode.rightPage == siblingPageNum)
        {
            currentNode.copyTo(oldChild);
            currentNode.copyTo(parentReplaceNode);
            memcpy(&parentReplaceNode.rightPage, (char *)siblingPage, sizeof(PageNum));
            parentPageReplaceSlotNum = i;
            free(currentNode.keyRidPair.key);
            break;
        }
        free(currentNode.keyRidPair.key);
    }

    if (parentPageReplaceSlotNum == (unsigned)NULL_INDICATOR)
    {
        return -1;
    }

    unsigned totalSize = 0;
    IX_Page siblingPageForamt(siblingPage);
    siblingPageForamt.deepLoadHeader(siblingPage);
    for (unsigned i = 0; i < siblingPageForamt.numberOfSlots; i++)
    {
        IX_NonLeafNode currentNode(siblingPage, siblingPageForamt.slotDirectory[i]);
        totalSize += currentNode.getNodeSize();
        free(currentNode.keyRidPair.key);
    }
    totalSize += parentReplaceNode.getNodeSize();

    IX_Page pageFormat(page);
    if (totalSize > (pageFormat.freeSpace - (siblingPageForamt.numberOfSlots + 1) * (SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE)))
    {
        return 0;
    }

    if (insertNonLeafNode(attribute, page, parentReplaceNode) != 0)
    {
        return -1;
    }

    for (unsigned i = 0; i < siblingPageForamt.numberOfSlots; i++)
    {
        IX_NonLeafNode currentNode(siblingPage, siblingPageForamt.slotDirectory[i]);
        if (insertNonLeafNode(attribute, page, currentNode) != 0)
        {
            free(currentNode.keyRidPair.key);
            return -1;
        }
        free(currentNode.keyRidPair.key);
    }

    memset(siblingPage, 0, PAGE_SIZE);

    return 0;
}

RC IndexManager::getSiblingPage(void *page, const PageNum &pageNum, void *parentPage, PageNum &siblingPageNum, bool &isLeft)
{
    IX_Page parentPageFormat(parentPage);
    parentPageFormat.deepLoadHeader(parentPage);

    PageNum leftPage = 0, rightPage = (unsigned)NULL_INDICATOR;
    memcpy(&leftPage, (char *)parentPage, sizeof(PageNum));

    for (unsigned i = 0; i < parentPageFormat.numberOfSlots; i++)
    {
        IX_NonLeafNode currentNode(parentPage, parentPageFormat.slotDirectory[i]);
        rightPage = currentNode.rightPage;

        if (leftPage == pageNum)
        {
            siblingPageNum = rightPage;
            isLeft = false;
            free(currentNode.keyRidPair.key);
            return 0;
        }

        if (i == parentPageFormat.numberOfSlots - 1)
        {
            if (rightPage == pageNum)
            {
                siblingPageNum = leftPage;
                isLeft = true;
                free(currentNode.keyRidPair.key);
                return 0;
            }
        }

        leftPage = rightPage;
        free(currentNode.keyRidPair.key);
    }

    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    PageNum rootPageNum;

    if (ixfileHandle.root == (unsigned)NULL_INDICATOR)
    {
        rootPageNum = 0;
        initializeLeafPage(page);
    }
    else
    {
        getRootPage(ixfileHandle, page);
        rootPageNum = ixfileHandle.root;
    }

    IX_NonLeafNode oldChild;
    if (deleteRoutine(ixfileHandle, attribute, key, rid, page, rootPageNum, page, rootPageNum, oldChild) == 0)
    {
        if (oldChild.rightPage != (unsigned)NULL_INDICATOR)
        {
            IX_Page pageFormat(page);

            if (pageFormat.numberOfSlots == 0)
            {
                PageNum newPageNum = 0;
                memcpy(&newPageNum, (char *)page, sizeof(PageNum));
                ixfileHandle.root = newPageNum;
                ixfileHandle.updateMetadata();
                memset(page, 0, PAGE_SIZE);
                // Kill old root.
                ixfileHandle.fh.writePage(rootPageNum, page);
            }
        }

        free(page);
        return 0;
    }

    free(page);
    return -1;
}

RC IndexManager::deleteRoutine(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid,
                               void *parentPage, const PageNum &parentPageNum, void *page, const PageNum &pageNum, IX_NonLeafNode &oldChild)
{
    unsigned occupancyThreshold = (unsigned)ceil(ixfileHandle.occupancy * 0.01 * PAGE_SIZE);

    if (IsLeafPage(page))
    {
        IX_LeafNode node(attribute, key, rid);
        if (deletedLeafNode(attribute, page, node) != 0)
        {
            free(node.key);
            return -1;
        }

        if(ixfileHandle.iSscanIteratorOpen())
        {
            if(ixfileHandle.currentRid.pageNum == pageNum && ixfileHandle.currentRid.slotNum != 0
                && ixfileHandle.currentRid.slotNum != (unsigned)NULL_INDICATOR)
            {
                ixfileHandle.currentRid.slotNum--;
            }
        }

        ixfileHandle.fh.writePage(pageNum, page);

        IX_Page pageFormat(page);
        if ((parentPageNum == pageNum) || (pageFormat.freeSpace <= occupancyThreshold))
        {
            oldChild.rightPage = (unsigned)NULL_INDICATOR;
            free(node.key);
            return 0;
        }
        else
        {
            void *siblingPage = malloc(PAGE_SIZE);
            memset(siblingPage, 0, PAGE_SIZE);
            PageNum siblingPageNum = pageFormat.nextPage;
            bool isLeft;
            if (getSiblingPage(page, pageNum, parentPage, siblingPageNum, isLeft) != 0)
            {
                free(siblingPage);
                oldChild.rightPage = (unsigned)NULL_INDICATOR;
                return 0;
            }

            if (siblingPageNum == (unsigned)NULL_INDICATOR)
            {
                free(siblingPage);
                free(node.key);
                oldChild.rightPage = (unsigned)NULL_INDICATOR;
                return 0;
            }

            if (ixfileHandle.fh.readPage(siblingPageNum, siblingPage) == 0)
            {
                IX_Page siblingPageFormat(siblingPage);
                if (!((PAGE_SIZE - pageFormat.freeSpace) + (PAGE_SIZE - siblingPageFormat.freeSpace) < PAGE_SIZE)
                        && (PAGE_SIZE - pageFormat.freeSpace) < (PAGE_SIZE - siblingPageFormat.freeSpace))
                {
                    if (isLeft)
                    {
                        RC redisRC = redistributeLeafPage(attribute, occupancyThreshold, siblingPage, page, pageNum, parentPage);
                        if (redisRC == 0)
                        {
                            IX_Page siblingPageFormatAfterRedis(siblingPage);
                            if(ixfileHandle.iSscanIteratorOpen() && ixfileHandle.currentRid.pageNum == pageNum 
                                        && siblingPageFormat.numberOfSlots - siblingPageFormatAfterRedis.numberOfSlots > 0)
                            {
                                ixfileHandle.currentRid.slotNum += siblingPageFormat.numberOfSlots 
                                                                    - siblingPageFormatAfterRedis.numberOfSlots;
                            }
                            ixfileHandle.fh.writePage(siblingPageNum, siblingPage);
                            ixfileHandle.fh.writePage(pageNum, page);
                            ixfileHandle.fh.writePage(parentPageNum, parentPage);
                            oldChild.rightPage = (unsigned)NULL_INDICATOR;
                            free(siblingPage);
                            free(node.key);
                            return 0;
                        }
                        else if (redisRC == -2)
                        {
                            oldChild.rightPage = (unsigned)NULL_INDICATOR;
                            free(siblingPage);
                            free(node.key);
                            return 0;
                        }
                    }
                    else
                    {
                        RC redisRC = redistributeLeafPage(attribute, occupancyThreshold, page, siblingPage, siblingPageNum, parentPage);
                        if (redisRC == 0)
                        {
                            IX_Page pageFormatAfterRedis(page);
                            if(ixfileHandle.iSscanIteratorOpen() && ixfileHandle.currentRid.pageNum == siblingPageNum 
                                        && pageFormatAfterRedis.numberOfSlots - pageFormat.numberOfSlots > 0)
                            {
                                if(pageFormatAfterRedis.numberOfSlots - pageFormat.numberOfSlots >= ixfileHandle.currentRid.slotNum + 1)
                                {
                                    ixfileHandle.currentRid.slotNum += pageFormat.numberOfSlots;
                                    ixfileHandle.currentRid.pageNum = pageNum;
                                }
                                else
                                {
                                    ixfileHandle.currentRid.slotNum -= pageFormatAfterRedis.numberOfSlots - pageFormat.numberOfSlots;
                                }
                            }
                            ixfileHandle.fh.writePage(siblingPageNum, siblingPage);
                            ixfileHandle.fh.writePage(pageNum, page);
                            ixfileHandle.fh.writePage(parentPageNum, parentPage);
                            oldChild.rightPage = (unsigned)NULL_INDICATOR;
                            free(siblingPage);
                            free(node.key);
                            return 0;
                        }
                        else if (redisRC == -2)
                        {
                            oldChild.rightPage = (unsigned)NULL_INDICATOR;
                            free(siblingPage);
                            free(node.key);
                            return 0;
                        }
                    }
                }
                else
                {
                    if ((PAGE_SIZE - pageFormat.freeSpace) + (PAGE_SIZE - siblingPageFormat.freeSpace) < PAGE_SIZE)
                    {
                        if (isLeft)
                        {
                            if (mergeLeafNode(attribute, siblingPage, pageNum, page, parentPage, oldChild) == 0)
                            {
                                if(ixfileHandle.iSscanIteratorOpen() && ixfileHandle.currentRid.pageNum == pageNum)
                                {
                                    ixfileHandle.currentRid.pageNum = siblingPageNum;
                                    ixfileHandle.currentRid.slotNum += siblingPageFormat.numberOfSlots;
                                }
                                
                                ixfileHandle.fh.writePage(pageNum, page);
                                ixfileHandle.fh.writePage(siblingPageNum, siblingPage);
                                free(siblingPage);
                                free(node.key);
                                return 0;
                            }
                        }
                        else
                        {
                            if (mergeLeafNode(attribute, page, siblingPageNum, siblingPage, parentPage, oldChild) == 0)
                            {
                                if(ixfileHandle.iSscanIteratorOpen() && ixfileHandle.currentRid.pageNum == siblingPageNum) 
                                {
                                    ixfileHandle.currentRid.pageNum = pageNum;
                                    ixfileHandle.currentRid.slotNum += pageFormat.numberOfSlots; 
                                }
                                ixfileHandle.fh.writePage(pageNum, page);
                                ixfileHandle.fh.writePage(siblingPageNum, siblingPage);
                                free(siblingPage);
                                free(node.key);
                                return 0;
                            }
                        }
                    }
                    else
                    {
                        ixfileHandle.fh.writePage(pageNum, page);
                        oldChild.rightPage = (unsigned)NULL_INDICATOR;
                        free(siblingPage);
                        free(node.key);
                        return 0;
                    }
                }
            }

            free(siblingPage);
        }
        free(node.key);
    }
    else
    {
        PageNum newPageNum = 0;
        if (findPage(attribute, key, rid, page, newPageNum) == 0)
        {
            void *newPage = malloc(PAGE_SIZE);
            memset(newPage, 0, PAGE_SIZE);
            ixfileHandle.fh.readPage(newPageNum, newPage);

            if (deleteRoutine(ixfileHandle, attribute, key, rid, page, pageNum, newPage, newPageNum, oldChild) == 0)
            {
                if (oldChild.rightPage == (unsigned)NULL_INDICATOR)
                {
                    free(newPage);
                    return 0;
                }
                else
                {
                    if (deletedNonLeafNode(attribute, page, oldChild) != 0)
                    {
                        free(newPage);
                        return -1;
                    }

                    ixfileHandle.fh.writePage(pageNum, page);

                    IX_Page pageFormat(page);
                    if (pageFormat.numberOfSlots == 0)
                    {
                        return 0;
                    }

                    if (pageFormat.freeSpace <= occupancyThreshold)
                    {
                        oldChild.rightPage = (unsigned)NULL_INDICATOR;
                        return 0;
                    }
                    else
                    {
                        void *siblingPage = malloc(PAGE_SIZE);
                        memset(siblingPage, 0, PAGE_SIZE);

                        PageNum siblingPageNum;
                        bool isLeft = true;
                        if (getSiblingPage(page, pageNum, parentPage, siblingPageNum, isLeft) != 0)
                        {
                            free(newPage);
                            free(siblingPage);
                            oldChild.rightPage = (unsigned)NULL_INDICATOR;
                            return 0;
                        }

                        if (siblingPageNum != (unsigned)NULL_INDICATOR && ixfileHandle.fh.readPage(siblingPageNum, siblingPage) == 0)
                        {
                            IX_Page siblingPageFormat(siblingPage);
                            if (!((PAGE_SIZE - pageFormat.freeSpace) + (PAGE_SIZE - siblingPageFormat.freeSpace) < PAGE_SIZE)
                                    && (PAGE_SIZE - pageFormat.freeSpace) < (PAGE_SIZE - siblingPageFormat.freeSpace))
                            {
                                if (isLeft)
                                {
                                    RC redisRC = redistributeNonLeafPage(attribute, occupancyThreshold, siblingPage,
                                                                         page, pageNum, parentPage);
                                    if (redisRC == 0)
                                    {
                                        ixfileHandle.fh.writePage(siblingPageNum, siblingPage);
                                        ixfileHandle.fh.writePage(pageNum, page);
                                        ixfileHandle.fh.writePage(parentPageNum, parentPage);
                                        oldChild.rightPage = (unsigned)NULL_INDICATOR;
                                        free(newPage);
                                        free(siblingPage);
                                        return 0;
                                    }
                                    else if (redisRC == -2)
                                    {
                                        oldChild.rightPage = (unsigned)NULL_INDICATOR;
                                        free(newPage);
                                        free(siblingPage);
                                        return 0;
                                    }
                                }
                                else
                                {
                                    RC redisRC = redistributeNonLeafPage(attribute, occupancyThreshold, page,
                                                                         siblingPage, siblingPageNum, parentPage);
                                    if (redisRC == 0)
                                    {
                                        ixfileHandle.fh.writePage(siblingPageNum, siblingPage);
                                        ixfileHandle.fh.writePage(pageNum, page);
                                        ixfileHandle.fh.writePage(parentPageNum, parentPage);
                                        oldChild.rightPage = (unsigned)NULL_INDICATOR;
                                        free(newPage);
                                        free(siblingPage);
                                        return 0;
                                    }
                                    else if (redisRC == -2)
                                    {
                                        oldChild.rightPage = (unsigned)NULL_INDICATOR;
                                        free(newPage);
                                        free(siblingPage);
                                        return 0;
                                    }
                                }
                            }
                            else
                            {
                                if ((PAGE_SIZE - pageFormat.freeSpace) + (PAGE_SIZE - siblingPageFormat.freeSpace) < PAGE_SIZE)
                                {
                                    if (isLeft)
                                    {
                                        if (mergeNonLeafNode(attribute, siblingPage, pageNum, page, parentPage, oldChild) == 0)
                                        {
                                            ixfileHandle.fh.writePage(siblingPageNum, siblingPage);
                                            // Kill current page.
                                            ixfileHandle.fh.writePage(pageNum, page);
                                            free(newPage);
                                            free(siblingPage);
                                            return 0;
                                        }
                                    }
                                    else
                                    {
                                        if (mergeNonLeafNode(attribute, page, siblingPageNum, siblingPage, parentPage, oldChild) == 0)
                                        {
                                            ixfileHandle.fh.writePage(pageNum, page);
                                            // Kill sibling page.
                                            ixfileHandle.fh.writePage(siblingPageNum, siblingPage);
                                            free(newPage);
                                            free(siblingPage);
                                            return 0;
                                        }
                                    }
                                }
                                else
                                {
                                    ixfileHandle.fh.writePage(pageNum, page);
                                    oldChild.rightPage = (unsigned)NULL_INDICATOR;
                                    free(newPage);
                                    free(siblingPage);
                                    return 0;
                                }
                            }
                        }

                        free(siblingPage);
                    }
                }
            }

            free(newPage);
        }
    }
    return -1;
}

RC IndexManager::findPage(const Attribute &attribute, const void *key, const RID &rid, const void *page, PageNum &returnPageNum)
{
    IX_Page currentPage(page);
    currentPage.deepLoadHeader(page);

    if (currentPage.numberOfSlots > 0)
    {
        IX_NonLeafNode firstNode(page, currentPage.slotDirectory[0]);
        if (firstNode.keyRidPair.compare(attribute, key, rid) < 0)
        {
            returnPageNum = firstNode.leftPage;
            free(firstNode.keyRidPair.key);
            return 0;
        }
        free(firstNode.keyRidPair.key);

        IX_NonLeafNode lastNode(page, currentPage.slotDirectory[currentPage.numberOfSlots - 1]);
        if (lastNode.keyRidPair.compare(attribute, key, rid) > 0)
        {
            returnPageNum = lastNode.rightPage;
            free(lastNode.keyRidPair.key);
            return 0;
        }
        free(lastNode.keyRidPair.key);

        for (int i = 0; i < currentPage.numberOfSlots - 1; i++)
        {
            IX_NonLeafNode nodeI(page, currentPage.slotDirectory[i]);
            IX_NonLeafNode nodeIplus1(page, currentPage.slotDirectory[i + 1]);

            if (nodeI.keyRidPair.compare(attribute, key, rid) > 0 && nodeIplus1.keyRidPair.compare(attribute, key, rid) < 0)
            {
                returnPageNum = nodeI.rightPage;
                free(nodeI.keyRidPair.key);
                free(nodeIplus1.keyRidPair.key);
                return 0;
            }
            else if (nodeI.keyRidPair.compare(attribute, key, rid) == 0)
            {
                returnPageNum = nodeI.rightPage;
                free(nodeI.keyRidPair.key);
                free(nodeIplus1.keyRidPair.key);
                return 0;
            }
            free(nodeI.keyRidPair.key);
            free(nodeIplus1.keyRidPair.key);
        }
    }

    return -1;
}

RC IndexManager::findSlotNum(const Attribute &attribute, const void *key, const void *page, unsigned &slotNum)
{
    if (!IsLeafPage(page))
    {
        return -1;
    }

    IX_Page currentPage(page);
    currentPage.deepLoadHeader(page);

    if (currentPage.numberOfSlots > 0)
    {
        IX_LeafNode firstNode(page, currentPage.slotDirectory[0]);
        if (firstNode.compare(attribute, key) < 0)
        {
            slotNum = 0;
            free(firstNode.key);
            return 0;
        }
        free(firstNode.key);

        IX_LeafNode lastNode(page, currentPage.slotDirectory[currentPage.numberOfSlots - 1]);
        if (lastNode.compare(attribute, key) > 0)
        {
            slotNum = currentPage.numberOfSlots;
            free(lastNode.key);
            return 0;
        }
        else if(lastNode.compare(attribute, key) == 0)
        {
            slotNum = currentPage.numberOfSlots - 1;
            free(lastNode.key);
            return 0;
        }
        free(lastNode.key);

        for (int i = 0; i < currentPage.numberOfSlots - 1; i++)
        {
            IX_LeafNode nodeI(page, currentPage.slotDirectory[i]);
            IX_LeafNode nodeIplus1(page, currentPage.slotDirectory[i + 1]);

            if (nodeI.compare(attribute, key) > 0 && nodeIplus1.compare(attribute, key) < 0)
            {
                slotNum = i + 1;
                free(nodeI.key);
                free(nodeIplus1.key);
                return 0;
            }
            else if (nodeI.compare(attribute, key) == 0)
            {
                slotNum = i;
                free(nodeI.key);
                free(nodeIplus1.key);
                return 0;
            }
            free(nodeI.key);
            free(nodeIplus1.key);
        }
    }

    slotNum == (unsigned)NULL_INDICATOR;
    return 0;
}

RC IndexManager::searchRoutine(IXFileHandle &ixfileHandle, const Attribute &attribute,
                               const void *key, const RID &rid, void *page, PageNum &pageNum)
{
    if (IsLeafPage(page))
    {
        return 0;
    }
    else
    {
        PageNum newPageNum;
        if (findPage(attribute, key, rid, page, newPageNum) == 0)
        {
            void *newPage = malloc(PAGE_SIZE);
            memset(newPage, 0, PAGE_SIZE);
            ixfileHandle.fh.readPage(newPageNum, newPage);

            if (searchRoutine(ixfileHandle, attribute, key, rid, newPage, newPageNum) == 0)
            {
                memset(page, 0, PAGE_SIZE);
                memcpy((char*)page, (char*)newPage, PAGE_SIZE);
                pageNum = newPageNum;
                free(newPage);
                return 0;
            }

            free(newPage);
        }
    }

    return -1;
}

RC IndexManager::search(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, RID &rid)
{
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);

    if (getRootPage(ixfileHandle, page) == 0)
    {
        rid.pageNum = ixfileHandle.root;
        RID searchOnRid;
        searchOnRid.pageNum = 0;
        searchOnRid.slotNum = 0;
        if (searchRoutine(ixfileHandle, attribute, key, searchOnRid, page, rid.pageNum) == 0)
        {
            if (findSlotNum(attribute, key, page, rid.slotNum) == 0)
            {
                free(page);
                return 0;
            }
        }
    }

    free(page);
    return -1;
}

RC IndexManager::getRootPage(IXFileHandle &ixfileHandle, void *page) const
{
    if (ixfileHandle.fh.readPage(ixfileHandle.root, page) == 0)
    {
        return 0;
    }

    return -1;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator)
{
    RID rid;
    if (search(ixfileHandle, attribute, lowKey, rid) == 0)
    {
        if (rid.pageNum == (unsigned)NULL_INDICATOR || rid.slotNum == (unsigned)NULL_INDICATOR)
        {
            rid.pageNum == (unsigned)NULL_INDICATOR;
            rid.slotNum == (unsigned)NULL_INDICATOR;
        }

        ix_ScanIterator.initialize(ixfileHandle, attribute, (void*)lowKey, 
                                    (void*)highKey, lowKeyInclusive, highKeyInclusive, rid);
        return 0;
    }

    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    string tree = "";

    if (getRootPage(ixfileHandle, page) == 0)
    {
        printRoutine(ixfileHandle, attribute, page, ixfileHandle.root, tree);

        FILE *treeJsonFile = fopen("treeJson", "w+");
        if (treeJsonFile != NULL)
        {
            fwrite(tree.c_str(), 1, tree.length(), treeJsonFile);
            fflush(treeJsonFile);
            fclose(treeJsonFile);
        }

        cout << tree.c_str() << endl;
        free(page);
        return;
    }

    free(page);
    return;
}

void IndexManager::printRoutine(IXFileHandle &ixfileHandle, const Attribute &attribute,
                                void *page, const PageNum &pageNum, string &tree) const
{
    IX_Page pageFormat(page);
    pageFormat.deepLoadHeader(page);

    string keys = "\"keys\":[";
    string children = "\"children\":[";

    if (pageFormat.isLeaf)
    {
        for (unsigned i = 0; i < pageFormat.numberOfSlots; i++)
        {
            string nodeString = "";
            IX_LeafNode node(page, pageFormat.slotDirectory[i]);
            node.realize(attribute, nodeString);
            if (i != 0)
            {
                keys.append(",");
            }
            keys.append("\"" + nodeString + "\"");
            free(node.key);
        }
    }
    else
    {
        bool isFirst = true;
        for (unsigned i = 0; i < pageFormat.numberOfSlots; i++)
        {
            if (isFirst)
            {
                PageNum newPageNum = 0;
                memcpy(&newPageNum, (char *)page, sizeof(PageNum));

                void *newPage = malloc(PAGE_SIZE);
                memset(newPage, 0, PAGE_SIZE);
                if(newPageNum != (unsigned)NULL_INDICATOR && ixfileHandle.fh.readPage(newPageNum, newPage) == 0)
                {
                    string newTree = "";
                    printRoutine(ixfileHandle, attribute, newPage, newPageNum, newTree);
                    children.append(newTree);
                }
                free(newPage);
                isFirst = false;
            }
            string nodeString = "";
            IX_NonLeafNode node(page, pageFormat.slotDirectory[i]);
            node.keyRidPair.realize(attribute, nodeString);
            if (i != 0)
            {
                keys.append(",");
            }
            keys.append("\"" + nodeString + "\"");

            void *newPage = malloc(PAGE_SIZE);
            memset(newPage, 0, PAGE_SIZE);
            if(node.rightPage != (unsigned)NULL_INDICATOR && ixfileHandle.fh.readPage(node.rightPage, newPage) == 0)
            {
                string newTree = "";
                printRoutine(ixfileHandle, attribute, newPage, node.rightPage, newTree);
                children.append(",");
                children.append(newTree);
            }
            free(newPage);
            free(node.keyRidPair.key);
        }
    }

    keys.append("]");
    children.append("]");

    tree.append("{");
    tree.append(keys);
    if (!pageFormat.isLeaf)
    {
        tree.append(",");
        tree.append(children);
    }
    tree.append("}");

    return;
}

// IX Page declarations
IX_Page::IX_Page(const void *page)
{
    freeSpace = 0;
    memcpy(&freeSpace, (char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE, PAGE_FREE_SPACE_SIZE);

    unsigned indicator = 0;
    memcpy(&indicator, (char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE, LEAF_NODE_INDICATOR_SIZE);

    if (indicator == (unsigned)1)
    {
        isLeaf = true;
        nextPage = (unsigned)NULL_INDICATOR;
        numberOfSlots = 0;
        memcpy(&nextPage, (char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NEXT_PAGE_POINTER_SIZE, NEXT_PAGE_POINTER_SIZE);
        memcpy(&numberOfSlots, (char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NEXT_PAGE_POINTER_SIZE - NUMBER_OF_SLOTS_SIZE, NUMBER_OF_SLOTS_SIZE);
    }
    else
    {
        isLeaf = false;
        nextPage = (unsigned)NULL_INDICATOR;
        numberOfSlots = 0;
        memcpy(&numberOfSlots, (char *)page + PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE - NUMBER_OF_SLOTS_SIZE, NUMBER_OF_SLOTS_SIZE);
    }
}

RC IX_Page::updateHeader(void *page)
{
    unsigned pointer = PAGE_SIZE;
    pointer -= PAGE_FREE_SPACE_SIZE;
    memcpy((char *)page + pointer, &freeSpace, PAGE_FREE_SPACE_SIZE);

    unsigned indicator = 0;
    if (isLeaf)
    {
        indicator = 1;
        pointer -= LEAF_NODE_INDICATOR_SIZE;
        memcpy((char *)page + pointer, &indicator, LEAF_NODE_INDICATOR_SIZE);

        pointer -= NEXT_PAGE_POINTER_SIZE;
        memcpy((char *)page + pointer, &nextPage, NEXT_PAGE_POINTER_SIZE);
        pointer -= NUMBER_OF_SLOTS_SIZE;
        memcpy((char *)page + pointer, &numberOfSlots, NUMBER_OF_SLOTS_SIZE);
    }
    else
    {
        indicator = 0;
        pointer -= LEAF_NODE_INDICATOR_SIZE;
        memcpy((char *)page + pointer, &indicator, LEAF_NODE_INDICATOR_SIZE);

        pointer -= NUMBER_OF_SLOTS_SIZE;
        memcpy((char *)page + pointer, &numberOfSlots, NUMBER_OF_SLOTS_SIZE);
    }

    if (numberOfSlots > 0)
    {
        if (slotDirectory.size() == numberOfSlots)
        {
            for (unsigned i = 0; i < numberOfSlots; i++)
            {
                pointer -= SLOT_LENGTH_SIZE;
                memcpy((char *)page + pointer, &slotDirectory[i].slotLen, SLOT_LENGTH_SIZE);
                pointer -= SLOT_OFFSET_SIZE;
                memcpy((char *)page + pointer, &slotDirectory[i].slotOffset, SLOT_OFFSET_SIZE);
            }
        }
        else
        {
            return -1;
        }
    }

    return 0;
}

RC IX_Page::deepLoadHeader(const void *page)
{
    if (numberOfSlots > 0)
    {
        unsigned pointer = PAGE_SIZE - PAGE_FREE_SPACE_SIZE - LEAF_NODE_INDICATOR_SIZE;
        if (isLeaf)
        {
            pointer -= NEXT_PAGE_POINTER_SIZE + NUMBER_OF_SLOTS_SIZE;
        }
        else
        {
            pointer -= NUMBER_OF_SLOTS_SIZE;
        }

        SlotEntry slotEntry;
        for (unsigned i = 0; i < numberOfSlots; i++)
        {
            pointer -= SLOT_LENGTH_SIZE;
            slotEntry.slotLen = 0;
            memcpy(&slotEntry.slotLen, (char *)page + pointer, SLOT_LENGTH_SIZE);
            pointer -= SLOT_OFFSET_SIZE;
            slotEntry.slotOffset = 0;
            memcpy(&slotEntry.slotOffset, (char *)page + pointer, SLOT_OFFSET_SIZE);
            slotDirectory.push_back(slotEntry);
        }

        return 0;
    }
    return -1;
}

// Is this required?
RC IX_Page::updateNode(const Attribute &attribute, void *page, IX_NonLeafNode node, unsigned &slotNum)
{
    if (slotNum >= 0 && slotNum < numberOfSlots)
    {
        if (attribute.type == TypeVarChar)
        {
        }
        else
        {
        }
    }
    return -1;
}

// IX Leaf node declarations
IX_LeafNode::IX_LeafNode(const void *page, const SlotEntry &slotEntry)
{
    unsigned pointer = slotEntry.slotOffset;
    keySize = slotEntry.slotLen - sizeof(PageNum) - SLOT_NUMBER_POINTER_IX;

    key = malloc(keySize);
    memset(key, 0, keySize);
    memcpy((char *)key, (char *)page + pointer, keySize);

    pointer += keySize;
    rid.pageNum = 0;
    memcpy(&rid.pageNum, (char *)page + pointer, sizeof(PageNum));

    pointer += sizeof(PageNum);
    rid.slotNum = 0;
    memcpy(&rid.slotNum, (char *)page + pointer, SLOT_NUMBER_POINTER_IX);

    size = slotEntry.slotLen;
}

IX_LeafNode::IX_LeafNode(const Attribute &attribute, const void *key, const RID &rid)
{
    this->rid.pageNum = rid.pageNum;
    this->rid.slotNum = rid.slotNum;

    unsigned keySize = attribute.length;
    if (attribute.type == TypeVarChar)
    {
        memcpy(&keySize, (char *)key, sizeof(unsigned));
        this->keySize = keySize;
        this->key = malloc(keySize);
        memset(this->key, 0, keySize);
        memcpy((char *)this->key, (char *)key + sizeof(unsigned), keySize);
    }
    else
    {
        this->keySize = keySize;
        this->key = malloc(keySize);
        memset(this->key, 0, keySize);
        memcpy((char *)this->key, (char *)key, keySize);
    }

    size = keySize + sizeof(PageNum) + SLOT_NUMBER_POINTER_IX;
}

RC IX_LeafNode::realize(const Attribute &attribute, string &nodeString)
{
    unsigned keySize = size - sizeof(PageNum) - SLOT_NUMBER_POINTER_IX;
    if (attribute.type == TypeVarChar)
    {
        char *keyVarChar = (char *)malloc(keySize + 1);
        memset(keyVarChar, 0, keySize + 1);
        memcpy(keyVarChar, (char *)key, keySize);
        *(keyVarChar + keySize) = '\0';

        nodeString.append(keyVarChar);
        free(keyVarChar);
    }
    else if (attribute.type == TypeInt)
    {
        unsigned keyInt = 0;
        memcpy(&keyInt, (char *)key, keySize);
        nodeString.append(to_string(keyInt));
    }
    else
    {
        float keyFloat = 0;
        memcpy(&keyFloat, (char *)key, keySize);
        nodeString.append(to_string(keyFloat));
    }

    nodeString.append(": [(" + to_string(rid.pageNum) + ", " + to_string(rid.slotNum) + ")]");
    return 0;
}

RC IX_LeafNode::copyTo(IX_LeafNode &node)
{
    keySize = size - sizeof(PageNum) - SLOT_NUMBER_POINTER_IX;
    node.key = malloc(keySize);
    memset(node.key, 0, keySize);
    memcpy((char *)node.key, this->key, keySize);

    node.rid.pageNum = this->rid.pageNum;
    node.rid.slotNum = this->rid.slotNum;
    node.size = keySize + sizeof(PageNum) + SLOT_NUMBER_POINTER_IX;
    node.keySize = keySize;
    return 0;
}

int IX_LeafNode::compare(const Attribute &attribute, IX_LeafNode &node, bool keyOnly = false)
{
    int compareResult = 0;
    if (attribute.type == TypeVarChar)
    {
        void *preparedKey = malloc(node.keySize + sizeof(int));
        memset(preparedKey, 0, node.keySize + sizeof(int));
        memcpy((char *)preparedKey, &node.keySize, sizeof(int));
        memcpy((char *)preparedKey + sizeof(int), (char *)node.key, node.keySize);

        if (keyOnly)
        {
            compareResult = compare(attribute, preparedKey);
        }
        else
        {
            compareResult = compare(attribute, preparedKey, node.rid);
        }

        free(preparedKey);
    }
    else
    {
        if (keyOnly)
        {
            compareResult = compare(attribute, node.key);
        }
        else
        {
            compareResult = compare(attribute, node.key, node.rid);
        }
    }

    return compareResult;
}

int IX_LeafNode::compare(const Attribute &attribute, const void *key)
{
    if (key == NULL)
    {
        return -1;
    }

    int compareResult;
    unsigned key1Size = size - sizeof(PageNum) - SLOT_NUMBER_POINTER_IX;
    unsigned key2Size = attribute.length;
    if (attribute.type == TypeVarChar)
    {
        memcpy(&key2Size, (char *)key, sizeof(unsigned));
        unsigned compareLen = max(key1Size, key2Size);
        char *stringKey1 = (char *)malloc(compareLen + 1);
        char *stringKey2 = (char *)malloc(compareLen + 1);
        memset(stringKey1, '\0', compareLen + 1);
        memset(stringKey2, '\0', compareLen + 1);
        memcpy(stringKey1, (char *)this->key, key1Size);
        memcpy(stringKey2, (char *)key + sizeof(unsigned), key2Size);

        compareResult = strcmp(stringKey2, stringKey1);
        free(stringKey1);
        free(stringKey2);
    }
    else if (attribute.type == TypeInt)
    {
        unsigned intKey1 = 0, intKey2 = 0;
        memcpy(&intKey1, (char *)this->key, key1Size);
        memcpy(&intKey2, (char *)key, key2Size);

        compareResult = intKey2 - intKey1;
    }
    else
    {
        float floatKey1 = 0, floatKey2 = 0;
        memcpy(&floatKey1, (char *)this->key, key1Size);
        memcpy(&floatKey2, (char *)key, key2Size);

        double compareResultDouble = floatKey2 - floatKey1;

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

int IX_LeafNode::compare(const Attribute &attribute, const void *key, const RID &rid)
{
    int compareResult = compare(attribute, key);

    if (compareResult == 0)
    {
        compareResult = rid.pageNum - this->rid.pageNum;
    }

    if (compareResult == 0)
    {
        compareResult = rid.slotNum - this->rid.slotNum;
    }

    return compareResult;
}

RC IX_LeafNode::returnKey(const Attribute &attribute, void *key)
{
    unsigned keySize = size - sizeof(PageNum) - SLOT_NUMBER_POINTER_IX;
    if (attribute.type == TypeVarChar)
    {
        memcpy((char *)key, &keySize, sizeof(int));
        memcpy((char *)key + sizeof(int), (char *)this->key, keySize);
    }
    else
    {
        memcpy((char *)key, (char *)this->key, keySize);
    }

    return 0;
}

RC IX_LeafNode::fabricateNode(void *keyRid)
{
    unsigned keySize = size - sizeof(PageNum) - SLOT_NUMBER_POINTER_IX;
    memset(keyRid, 0, size);
    memcpy((char *)keyRid, (char *)key, keySize);
    memcpy((char *)keyRid + keySize, &rid.pageNum, sizeof(PageNum));
    memcpy((char *)keyRid + keySize + sizeof(PageNum), &rid.slotNum, SLOT_NUMBER_POINTER_IX);

    return 0;
}

unsigned IX_LeafNode::getNodeSize() const
{
    return size;
}

IX_LeafNode::IX_LeafNode()
{
}

IX_LeafNode::~IX_LeafNode()
{
}

// IX NonLeaf node declarations
IX_NonLeafNode::IX_NonLeafNode()
{
    leftPage = (unsigned)NULL_INDICATOR;
    rightPage = (unsigned)NULL_INDICATOR;
}

IX_NonLeafNode::IX_NonLeafNode(const void *page, const SlotEntry &slotEntry)
{
    SlotEntry keyRidSlotEntry;
    keyRidSlotEntry.slotOffset = slotEntry.slotOffset;
    keyRidSlotEntry.slotLen = slotEntry.slotLen - sizeof(PageNum);

    IX_LeafNode node(page, keyRidSlotEntry);
    node.copyTo(keyRidPair);

    rightPage = 0;
    memcpy(&rightPage, (char *)page + slotEntry.slotOffset + keyRidSlotEntry.slotLen, sizeof(PageNum));
    if (slotEntry.slotOffset == sizeof(PageNum))
    {
        memcpy(&leftPage, (char *)page, sizeof(PageNum));
    }
    else
    {
        leftPage = (unsigned)NULL_INDICATOR;
    }

    size = node.getNodeSize() + sizeof(PageNum);
    free(node.key);
}

IX_NonLeafNode::IX_NonLeafNode(IX_LeafNode &keyRidPair, PageNum &rightPage)
{
    keyRidPair.copyTo(this->keyRidPair);
    this->rightPage = rightPage;
    size = keyRidPair.getNodeSize() + sizeof(PageNum);
}

RC IX_NonLeafNode::copyTo(IX_NonLeafNode &node)
{
    keyRidPair.copyTo(node.keyRidPair);
    node.rightPage = rightPage;
    node.leftPage = leftPage;
    node.size = size;
    return 0;
}

RC IX_NonLeafNode::fabricateNode(void *entry)
{
    void *keyRid = malloc(keyRidPair.getNodeSize());
    memset(keyRid, 0, keyRidPair.getNodeSize());
    keyRidPair.fabricateNode(keyRid);

    memset(entry, 0, keyRidPair.getNodeSize() + sizeof(PageNum));
    memcpy((char *)entry, (char *)keyRid, keyRidPair.getNodeSize());
    memcpy((char *)entry + keyRidPair.getNodeSize(), &rightPage, sizeof(PageNum));

    free(keyRid);
    return 0;
}

unsigned IX_NonLeafNode::getNodeSize() const
{
    return keyRidPair.getNodeSize() + sizeof(PageNum);
}

// IX ScanIterator declarations
IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::initialize(IXFileHandle &ixfileHandle,
                               const Attribute &attribute,
                               void *lowKey,
                               void *highKey,
                               bool lowKeyInclusive,
                               bool highKeyInclusive,
                               RID &rid)
{
    this->ix = IndexManager::instance();
    ixfileHandle.currentRid.pageNum = rid.pageNum;
    ixfileHandle.currentRid.slotNum = rid.slotNum;
    this->ixfh = ixfileHandle;
    //this->ixfh.reset(&ixfileHandle);
    this->attribute.length = attribute.length;
    this->attribute.name = attribute.name;
    this->attribute.type = attribute.type;
    this->lowKey = lowKey;
    this->highKey = highKey;
    this->lowKeyInclusive = lowKeyInclusive;
    this->highKeyInclusive = highKeyInclusive;
    return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    PageNum pageNum = ixfh.currentRid.pageNum;
    unsigned slotNum = ixfh.currentRid.slotNum;

    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);

    while (pageNum != (unsigned)NULL_INDICATOR && pageNum < ixfh.fh.getNumberOfPages() && ixfh.fh.readPage(pageNum, page) == 0)
    {
        IX_Page pageFormat(page);
        pageFormat.deepLoadHeader(page);

        if (!pageFormat.isLeaf)
        {
            free(page);
            return -1;
        }

        while ( slotNum < pageFormat.numberOfSlots)
        {
            IX_LeafNode currentNode(page, pageFormat.slotDirectory[slotNum]);

            if ((lowKey == NULL && highKey == NULL))
            {
                returnKey(currentNode, key, rid);
                setNextRid(++slotNum, pageNum, pageFormat.numberOfSlots, pageFormat.nextPage);
                free(page);
                free(currentNode.key);
                return 0;
            }
            else if (lowKey == NULL && (currentNode.compare(attribute, highKey) > 0 || (currentNode.compare(attribute, highKey) == 0 && highKeyInclusive)))
            {
                returnKey(currentNode, key, rid);
                setNextRid(++slotNum, pageNum, pageFormat.numberOfSlots, pageFormat.nextPage);
                free(page);
                free(currentNode.key);
                return 0;
            }
            else if ((currentNode.compare(attribute, lowKey) < 0 || (currentNode.compare(attribute, lowKey) == 0 && lowKeyInclusive)) && highKey == NULL)
            {
                returnKey(currentNode, key, rid);
                setNextRid(++slotNum, pageNum, pageFormat.numberOfSlots, pageFormat.nextPage);
                free(page);
                free(currentNode.key);
                return 0;
            }
            else if ((currentNode.compare(attribute, lowKey) < 0 || (currentNode.compare(attribute, lowKey) == 0 && lowKeyInclusive)) && (currentNode.compare(attribute, highKey) > 0 || (currentNode.compare(attribute, highKey) == 0 && highKeyInclusive)))
            {
                returnKey(currentNode, key, rid);
                setNextRid(++slotNum, pageNum, pageFormat.numberOfSlots, pageFormat.nextPage);
                free(page);
                free(currentNode.key);
                return 0;
            }
            else if(currentNode.compare(attribute, highKey) < 0)
            {
                free(page);
                free(currentNode.key);
                return -1;
            }
            free(currentNode.key);
            slotNum++;
        }
        pageNum = pageFormat.nextPage;
        slotNum = 0;
    }

    free(page);
    return -1;
}

RC IX_ScanIterator::setNextRid(unsigned &slotNum, unsigned &pageNum, 
            const unsigned &maxNumofSlots, const PageNum &nextPage )
{
    if(slotNum != (unsigned)NULL_INDICATOR) 
    {
        if(slotNum >= maxNumofSlots)
        {
            ixfh.currentRid.slotNum = 0;
            ixfh.currentRid.pageNum = nextPage;
        }
        else
        {
            ixfh.currentRid.slotNum = slotNum;
            ixfh.currentRid.pageNum = pageNum;
        }
        return 0;
    }
    return -1;
}

RC IX_ScanIterator::returnKey(IX_LeafNode node, void *key, RID &rid)
{
    unsigned keySize = attribute.length;
    if (attribute.type == TypeVarChar)
    {
        keySize = node.getNodeSize() - sizeof(PageNum) - SLOT_NUMBER_POINTER_IX + sizeof(int);
    }
    node.returnKey(attribute, key);
    rid.pageNum = node.rid.pageNum;
    rid.slotNum = node.rid.slotNum;
    return 0;
}

RC IX_ScanIterator::close()
{
    ixfh.currentRid.pageNum = (unsigned)NULL_INDICATOR;
    ixfh.currentRid.slotNum = (unsigned)NULL_INDICATOR;
    return 0;
}

// IX FileHandle declarations
IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    root = (unsigned)NULL_INDICATOR;
    occupancy = (unsigned)ceil((INDEX_OCCUPANCY_D * PAGE_SIZE) / 100);
    currentRid.slotNum = (unsigned)NULL_INDICATOR;
    currentRid.pageNum = (unsigned)NULL_INDICATOR;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::readMetadata()
{
    void *metaPage = malloc(PAGE_SIZE);
    memset(metaPage, 0, PAGE_SIZE);
    int pointer = 3 * sizeof(int);

    if (fh.readPage(-1, metaPage) == 0)
    {
        root = (unsigned)NULL_INDICATOR;
        memcpy(&root, (char *)metaPage + pointer, sizeof(int));
        pointer += sizeof(int);

        unsigned tempOccupany = 0;
        memcpy(&tempOccupany, (char *)metaPage + pointer, sizeof(int));
        pointer += sizeof(int);

        if (tempOccupany != 0)
        {
            occupancy = tempOccupany;
        }

        free(metaPage);
        return 0;
    }

    free(metaPage);
    return -1;
}

RC IXFileHandle::updateMetadata()
{
    void *metaPage = malloc(PAGE_SIZE);
    memset(metaPage, 0, PAGE_SIZE);
    int pointer = 3 * sizeof(int);

    if (fh.readPage(-1, metaPage) == 0)
    {
        memcpy((char *)metaPage + pointer, &root, sizeof(int));
        pointer += sizeof(int);

        int error = fh.writePage(-1, metaPage);
        free(metaPage);
        return error;
    }

    free(metaPage);
    return -1;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    ixReadPageCounter = fh.readPageCounter;
    ixWritePageCounter = fh.writePageCounter;
    ixAppendPageCounter = fh.appendPageCounter;

    readPageCount = fh.readPageCounter;
    writePageCount = fh.writePageCounter;
    appendPageCount = fh.appendPageCounter;

    return 0;
}

bool IXFileHandle::iSscanIteratorOpen()
{
    if(currentRid.pageNum == (unsigned)NULL_INDICATOR 
        || currentRid.slotNum == (unsigned)NULL_INDICATOR) 
    {
        return false;
    }

    return true;
}