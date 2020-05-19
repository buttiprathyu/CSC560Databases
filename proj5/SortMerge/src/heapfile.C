#include "heapfile.h"

// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {
    "bad record id",
    "bad record pointer",
    "end of file encountered",
    "invalid update operation",
    "no space on page for record",
    "page is empty - no records",
    "last record on page",
    "invalid slot number",
    "file has already been deleted",
};

static error_string_table hfTable(HEAPFILE, hfErrMsgs);

// ********************************************************
// Constructor
HeapFile::HeapFile(const char *name, Status &returnStatus)
{
    // MINIBASE_DB->dump_space_map();

    Status state;

    if (MINIBASE_DB->get_file_entry(name, firstDirPageId) == OK)
    {
       // cout << "The file already exists" << endl;
        fileName = (char *)malloc(strlen(name) + 1);
        strcpy(fileName, name);
        returnStatus = OK;
        // return;
    }

    else
    {
        Page *firstPage;
        state = MINIBASE_BM->newPage(firstDirPageId, firstPage, 1);
        state = MINIBASE_DB->add_file_entry(name, firstDirPageId);
        state = MINIBASE_DB->get_file_entry(name, firstDirPageId);

        HFPage hfp;
        hfp.init(firstDirPageId);
        memcpy(&(*firstPage), &hfp, 1024);
       fileName = (char *)malloc(strlen(name) + 1);
        strcpy(fileName, name);
        state = MINIBASE_BM->unpinPage(firstDirPageId, TRUE, fileName);
        returnStatus = OK;
    }
}

// ******************
// Destructor
HeapFile::~HeapFile()
{
    Status s;
    // Status s = MINIBASE_BM->flushAllPages();
   // s = MINIBASE_DB->delete_file_entry(fileName);
    // fill in the body
}

// *************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
    struct DataPageInfo *temp_DataPageInfo = new struct DataPageInfo;
    Page *t_page;                    //, *t1_page;
    PageId curr_pageid, next_pageid; // allocDirPageId,
    int record_count, t_recLen;
    HFPage hfp;
    struct RID cur_RID; //, allocDataPageRid, dirPage_rid;
    char *t_recPtr;
    Status state = OK;
    curr_pageid = firstDirPageId;
    record_count = 0;
    while (1)
    {
        state = MINIBASE_BM->pinPage(curr_pageid, t_page, 0, fileName);
        memcpy(&hfp, &(*t_page), 1024);
        cur_RID.pageNo = curr_pageid;
        state = hfp.firstRecord(cur_RID);
        while (state != DONE)
        {
            state = hfp.returnRecord(cur_RID, t_recPtr, t_recLen);
            temp_DataPageInfo = reinterpret_cast<struct DataPageInfo *>(t_recPtr);
            record_count = record_count + temp_DataPageInfo->recct;
            state = hfp.nextRecord(cur_RID, cur_RID);
        }
        //next directory page
        next_pageid = hfp.getNextPage();
        if (next_pageid == -1) //the page doesn't exist yet create it and than next page assignment
        {
            state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
            return record_count;
        }
        state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
        curr_pageid = next_pageid;
    }
    return -1;
    //return OK;
}

// *****************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID &outRid)
{
    // cout<<"In insert record";
    struct DataPageInfo Dummy = {0, 0, 0};

    struct DataPageInfo *temp_DataPageInfo; // = new struct DataPageInfo;
    temp_DataPageInfo = &Dummy;
    Page *t_page; //, *t1_page;
    PageId allocDirPageId, curr_pageid, next_pageid;
    int t_recLen; //freespace,
    HFPage hfp;
    bool first = TRUE;
    struct RID cur_RID, allocDataPageRid; //, dirPage_rid;
    char *t_recPtr;
    Status state = OK;
    curr_pageid = firstDirPageId;
    if (recLen > 1000)
    {
        MINIBASE_FIRST_ERROR(HEAPFILE, *hfErrMsgs[4]);
        return HEAPFILE;
    }

    while (1)
    {
        state = MINIBASE_BM->pinPage(curr_pageid, t_page, 0, fileName);
        memcpy(&hfp, &(*t_page), 1024);
        cur_RID.pageNo = curr_pageid;
        state = hfp.firstRecord(cur_RID);
        while ((temp_DataPageInfo->availspace < recLen && state != DONE))
        {
            state = hfp.returnRecord(cur_RID, t_recPtr, t_recLen);
            temp_DataPageInfo = reinterpret_cast<struct DataPageInfo *>(t_recPtr);
            state = hfp.nextRecord(cur_RID, cur_RID);
        }
        if (temp_DataPageInfo->availspace > recLen)
        {
            state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
            break;
        }

        if ((state == DONE && hfp.available_space() > sizeof(struct DataPageInfo)))
        {
            newDataPage(temp_DataPageInfo);
            allocateDirSpace(temp_DataPageInfo, allocDirPageId, allocDataPageRid);
            state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
            break;
        }
        //next directory page
        next_pageid = hfp.getNextPage();
        if (next_pageid == -1) //the page doesn't exist yet create it and than next page assignment
        {
            newDataPage(temp_DataPageInfo);
            allocateDirSpace(temp_DataPageInfo, allocDirPageId, allocDataPageRid);
            state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
            break;
        }
        state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
        curr_pageid = next_pageid;
    }
    //insert the page
    Page *insert_page;
    if (MINIBASE_BM->pinPage(temp_DataPageInfo->pageId, insert_page, 0, fileName) == OK)
    {
        HFPage insert_hfp;
        memcpy(&insert_hfp, &(*insert_page), 1024);
        state = insert_hfp.insertRecord(recPtr, recLen, outRid);
        temp_DataPageInfo->recct = temp_DataPageInfo->recct + 1;
        temp_DataPageInfo->availspace = insert_hfp.available_space();
        memcpy(&(*insert_page), &insert_hfp, 1024);
        state = MINIBASE_BM->unpinPage(temp_DataPageInfo->pageId, TRUE, fileName);
    }

    state = allocateDirSpace(temp_DataPageInfo, allocDirPageId, allocDataPageRid);
    // delete temp_DataPageInfo;
    return OK;
}

// ***********************
// delete record from file
Status HeapFile::deleteRecord(const RID &rid)
{
    Status state;
    char *t_recPtr;
    int t_len;
    PageId DirPageId, DataPageId;
    HFPage *DirPage, *DataPage;
    struct RID dirPage_rid;
    state = findDataPage(rid, DirPageId, DirPage, DataPageId, DataPage, dirPage_rid);
    //delete the record in data page
    state = DataPage->deleteRecord(rid);
    //update the directory
    state = DirPage->returnRecord(dirPage_rid, t_recPtr, t_len);
    struct DataPageInfo *t_DataPageInfo = reinterpret_cast<struct DataPageInfo *>(t_recPtr);
    t_DataPageInfo->recct = t_DataPageInfo->recct - 1;
    t_DataPageInfo->availspace = DataPage->available_space();
    //load back to DirPage
    memcpy(t_recPtr, t_DataPageInfo, sizeof(struct DataPageInfo));
    //unpin both the pages
    state = MINIBASE_BM->unpinPage(DataPageId, TRUE, fileName);
    state = MINIBASE_BM->unpinPage(DirPageId, TRUE, fileName);

    return OK;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord(const RID &rid, char *recPtr, int recLen)
{
    // fill in the body
    Status state;
    char *t_recPtr;
    int t_len;
    PageId DirPageId, DataPageId;
    HFPage *DirPage, *DataPage;
    struct RID dirPage_rid;
    state = findDataPage(rid, DirPageId, DirPage, DataPageId, DataPage, dirPage_rid);
    //update the record in data page
    state = DataPage->returnRecord(rid, t_recPtr, t_len);
    if (t_len != recLen)
    {
        MINIBASE_FIRST_ERROR(HEAPFILE, *hfErrMsgs[3]);
        return HEAPFILE;
    }

    memcpy(t_recPtr, recPtr, recLen);
    //update the directory
    state = DirPage->returnRecord(dirPage_rid, t_recPtr, t_len);
    struct DataPageInfo *t_DataPageInfo = reinterpret_cast<struct DataPageInfo *>(t_recPtr);

    t_DataPageInfo->availspace = DataPage->available_space();
    //load back to DirPage
    memcpy(t_recPtr, t_DataPageInfo, sizeof(struct DataPageInfo));
    //unpin both the pages
    state = MINIBASE_BM->unpinPage(DataPageId, TRUE, fileName);
    state = MINIBASE_BM->unpinPage(DirPageId, TRUE, fileName);

    return OK;
}

// ***************************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord(const RID &rid, char *recPtr, int &recLen)
{
    // fill in the body
    Status state;
    char *t_recPtr;
    int t_len;
    PageId DirPageId, DataPageId;
    HFPage *DirPage, *DataPage;
    struct RID dirPage_rid;
    state = findDataPage(rid, DirPageId, DirPage, DataPageId, DataPage, dirPage_rid);
    //update the record in data page
    state = DataPage->returnRecord(rid, t_recPtr, recLen);
    memcpy(recPtr, t_recPtr, recLen);
    //unpin both the pages
    state = MINIBASE_BM->unpinPage(DataPageId, TRUE, fileName);
    state = MINIBASE_BM->unpinPage(DirPageId, TRUE, fileName);

    return OK;
}

// **************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status &status)
{
    // fill in the body
    Scan *t_scan = new Scan(this, status);
    return t_scan;
    //return NULL;
}

// ****************************************************
// Wipes out the heapfile from the database permanently.
Status HeapFile::deleteFile()
{
    // fill in the body
    return OK;
    struct DataPageInfo *temp_DataPageInfo = new struct DataPageInfo;
    Page *t_page;                    //, *t1_page;
    PageId curr_pageid, next_pageid; // allocDirPageId,
    int record_count, t_recLen;
    HFPage hfp;
    struct RID cur_RID; //, allocDataPageRid, dirPage_rid;
    char *t_recPtr;
    Status state = OK;
    curr_pageid = firstDirPageId;
    record_count = 0;
    //data Page
    while (1)
    {
        state = MINIBASE_BM->pinPage(curr_pageid, t_page, 0, fileName);
        memcpy(&hfp, &(*t_page), 1024);
        cur_RID.pageNo = curr_pageid;
        state = hfp.firstRecord(cur_RID);
        while (state != DONE)
        {
            state = hfp.returnRecord(cur_RID, t_recPtr, t_recLen);
            temp_DataPageInfo = reinterpret_cast<struct DataPageInfo *>(t_recPtr);

            state = MINIBASE_BM->freePage(temp_DataPageInfo->pageId);
            if (state != OK)
            {
                state = MINIBASE_BM->unpinPage(temp_DataPageInfo->pageId, FALSE, fileName);
                while (state == OK)
                {
                    state = MINIBASE_BM->unpinPage(temp_DataPageInfo->pageId, FALSE, fileName);
                }
                state = MINIBASE_BM->freePage(temp_DataPageInfo->pageId);
            }
            state = hfp.nextRecord(cur_RID, cur_RID);
        }
        //next directory page
        next_pageid = hfp.getNextPage();
        // cout << "next_pageid " << next_pageid << endl;
        if (next_pageid == -1) //the page doesn't exist yet
        {
            state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
            break;
        }
        state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
        curr_pageid = next_pageid;
    }
    curr_pageid = firstDirPageId;
    while (1)
    {
        state = MINIBASE_BM->pinPage(curr_pageid, t_page, 0, fileName);
        memcpy(&hfp, &(*t_page), 1024);
        next_pageid = hfp.getNextPage();
        state = MINIBASE_BM->freePage(curr_pageid);
        if (state != OK)
        {
            state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
            while (state == OK)
            {
                state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
            }
            state = MINIBASE_BM->freePage(curr_pageid);
        }
        if (next_pageid == -1) //the page doesn't exist yet create it and than next page assignment
        {

            break;
        }
        state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
        curr_pageid = next_pageid;
    }

    return OK;
}

// ****************************************************************
// Get a new datapage from the buffer manager and initialize dpinfo
// (Allocate pages in the db file via buffer manager)
Status HeapFile::newDataPage(DataPageInfo *dpinfop)
{
    // fill in the body
    //look into directory and see if any empty page if yes than bring them on
    Page *newpage;
    int t_pageid;
    Status state;
    if (MINIBASE_BM->newPage(t_pageid, newpage, 1) == OK)
    {
        HFPage hfp;
        hfp.init(t_pageid);
        dpinfop->availspace = hfp.available_space();
        dpinfop->pageId = t_pageid;
        dpinfop->recct = 0;
        memcpy(&(*newpage), &hfp, 1024);
        state = MINIBASE_BM->unpinPage(t_pageid, TRUE, fileName);

        //allocate directory space for the page
        return OK;
    }
    return OK;
}
// ************************************************************************
// Internal HeapFile function (used in getRecord and updateRecord): returns
// pinned directory page and pinned data page of the specified user record
// (rid).
//
// If the user record cannot be found, rpdirpage and rpdatapage are
// returned as NULL pointers.
//
Status HeapFile::findDataPage(const RID &rid,
                              PageId &rpDirPageId, HFPage *&rpdirpage,
                              PageId &rpDataPageId, HFPage *&rpdatapage,
                              RID &rpDataPageRid)
{
    Page *t_page, *t1_page;
    HFPage hfp, t_hfp;
    struct RID t_first, temp_data;
    PageId next_pageid, data_pageid;
    PageId curr_pageid = firstDirPageId;
    rpDirPageId = firstDirPageId;
    Status state;
    char *t_recPtr;
    int t_recLen;
    struct DataPageInfo *t_DataPageInfo;
    while (1)
    {
        if (MINIBASE_BM->pinPage(rpDirPageId, t_page, 0, fileName) == OK)
        {
            memcpy(&hfp, &(*t_page), 1024);
            rpDataPageRid.pageNo = rpDirPageId;
            state = hfp.firstRecord(rpDataPageRid);
            if (state != OK)
            {
                cout << "Record Not found at first record " << rpDirPageId << endl;
                return DONE;
            }
            else
            {
                while (state == OK)
                {
                    state = hfp.returnRecord(rpDataPageRid, t_recPtr, t_recLen);
                    t_DataPageInfo = reinterpret_cast<struct DataPageInfo *>(t_recPtr);
                    state = MINIBASE_BM->pinPage(t_DataPageInfo->pageId, t1_page, 0, fileName);
                    memcpy(&t_hfp, &(*t1_page), 1024);
                    temp_data.pageNo = t_DataPageInfo->pageId;
                    state = t_hfp.firstRecord(temp_data);
                    while (state == OK)
                    {
                        //search the record
                        if (temp_data.pageNo == rid.pageNo)
                            if (temp_data.slotNo == rid.slotNo)
                            {

                                rpDataPageId = t_DataPageInfo->pageId;
                                rpdatapage = reinterpret_cast<HFPage *>(t1_page);
                                rpdirpage = reinterpret_cast<HFPage *>(t_page);
                                return OK;
                            }
                        state = t_hfp.nextRecord(temp_data, temp_data);
                    }
                    //done with that page..
                    state = MINIBASE_BM->unpinPage(t_DataPageInfo->pageId, FALSE, fileName);
                    //status of the next record in the page
                    state = hfp.nextRecord(rpDataPageRid, rpDataPageRid);
                }
            }
        }
        else
        {
            return DONE;
        }
        next_pageid = hfp.getNextPage();
        if (next_pageid == -1) //the page doesn't exist yet create it and than next page assignment
        {
            rpDirPageId = 0;
            rpDataPageId = 0;
            rpdatapage = NULL;
            rpdirpage = NULL;
            return DONE;
        }

        state = MINIBASE_BM->unpinPage(rpDirPageId, FALSE, fileName);
        rpDirPageId = next_pageid;
    }
    return DONE;
}

// *********************************************************************
// Allocate directory space for a heap file page

Status HeapFile::allocateDirSpace(struct DataPageInfo *dpinfop,
                                  PageId &allocDirPageId,
                                  RID &allocDataPageRid)
{
    Page *t_page, *t1_page;
    HFPage hfp;
    struct RID t_first, cur_RID;
    PageId next_pageid;
    PageId curr_pageid = firstDirPageId;
    int i, t_recLen;
    char *d, *t_recPtr; //[sizeof(*dpinfop)];
    Status state;
    struct DataPageInfo *t_DataPageInfo;
    while (1) //serach if the page if already exist
    {
        state = MINIBASE_BM->pinPage(curr_pageid, t_page, 0, fileName);
        memcpy(&hfp, &(*t_page), 1024);
        cur_RID.pageNo = curr_pageid;
        state = hfp.firstRecord(cur_RID);
        while (state == OK)
        {
            state = hfp.returnRecord(cur_RID, t_recPtr, t_recLen);
            t_DataPageInfo = reinterpret_cast<struct DataPageInfo *>(t_recPtr);
            if (t_DataPageInfo->pageId == dpinfop->pageId)
            {
                allocDirPageId = curr_pageid;
                allocDataPageRid = cur_RID;
                t_DataPageInfo->availspace = dpinfop->availspace;
                t_DataPageInfo->recct = dpinfop->recct;
                memcpy(t_recPtr, t_DataPageInfo, sizeof(struct DataPageInfo));
                memcpy(&(*t_page), &hfp, 1024);
                state = MINIBASE_BM->unpinPage(curr_pageid, TRUE, fileName);
                return OK;
            }
            state = hfp.nextRecord(cur_RID, cur_RID);
        }
        next_pageid = hfp.getNextPage();
        if (next_pageid == -1) //the page doesn't exist hence exit
        {
            state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
            break;
        }
        state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
        curr_pageid = next_pageid;
    }

    curr_pageid = firstDirPageId;
    while (1)
    {
        if (MINIBASE_BM->pinPage(curr_pageid, t_page, 0, fileName) == OK)
        {
            memcpy(&hfp, &(*t_page), 1024);
            if (hfp.available_space() > sizeof(*dpinfop))
            {
                char *recPtr = reinterpret_cast<char *>(dpinfop);
                hfp.insertRecord(recPtr, sizeof(struct DataPageInfo), t_first);
                //  cout << "pageid " << dpinfop->pageId << endl;
                hfp.returnRecord(t_first, d, i);
                struct DataPageInfo *temp = reinterpret_cast<struct DataPageInfo *>(d);
                //check if the correct value is reqtured
                hfp.firstRecord(allocDataPageRid);
                memcpy(&(*t_page), &(hfp), 1024);
                state = MINIBASE_BM->unpinPage(curr_pageid, TRUE, fileName);
                return OK;
            }
            else
            {
                next_pageid = hfp.getNextPage();
                if (next_pageid == -1) //the page doesn't exist yet create it and than next page assignment
                {
                    HFPage t_hfp;
                    state = MINIBASE_BM->newPage(next_pageid, t1_page, 1);
                    t_hfp.init(next_pageid);
                    memcpy(&(*t1_page), &t_hfp, 1024);
                    state = MINIBASE_BM->unpinPage(next_pageid, TRUE, fileName);
                    hfp.setNextPage(next_pageid);
                }
                memcpy(&(*t_page), &(hfp), 1024);
                state = MINIBASE_BM->unpinPage(curr_pageid, FALSE, fileName);
                curr_pageid = next_pageid;
            }
        }
    }
}

// *******************************************
