#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"

// **********************************************************
// page class constructor

BufMgr *BufManager = new BufMgr(5000, 0);

void HFPage::init(PageId pageNo)
{
    // fill in the body
    bool allocate_new_page = false;
    prevPage = -1;
    nextPage = -1;
    curPage = pageNo;
    usedPtr = 1000;
    slotCnt = 0;
    slot[1].length = 0;
    slot[1].offset = 1002;
    Page *p;

    allocate_new_page = BufManager->newPage(pageNo, p, 1);
    freeSpace = DPFIXED + sizeof(slot_t) * (1 - slotCnt);
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;

    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;

    for (i = 0; i < slotCnt; i++)
    {
        cout << "slot[" << i << "].offset=" << slot[i].offset
             << ", slot[" << i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
    // fill in the body
    return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{

    // fill in the body
    prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
    // fill in the body
    return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
    // fill in the body
    nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char *recPtr, int recLen, RID &rid)
{
    // fill in the body
    int space_available = available_space();
    bool slot_found = false;
    if (space_available >= (recLen))
    {
        //place the data into the usedPtr
        //data[usedPtr] = *recPtr;
        //data[usedPtr-recLen]=*recPtr;
        memcpy(&data[usedPtr - recLen], recPtr, recLen * sizeof(char));

        //place the page No in the rid
        rid.pageNo = curPage;
        //Check if any slot has empty offset indicating there is no record
        for (int i = 0; i < slotCnt; i++)
        {
            //find an empty slot with -1 offset
            if (slot[i].offset == -1)
            {
                //empty slot found... place the offset into this position that is usedPtrt -(bytes taken from usedPtr by the slots)
                slot[i].offset = usedPtr /*- (sizeof(slot_t)) * i*/;
                slot[i].length = recLen;
                //slot founc
                slot_found = true;
                //insert the slot No in the rid
                rid.slotNo = i;
            }
        }
        if (slot_found == false)
        {
            //we need a new slot
            //enter the new slot number into the rid
            rid.slotNo = slotCnt;
            slot[slotCnt].offset = usedPtr /*- sizeof(slot_t) * slotCnt*/;
            slot[slotCnt].length = recLen;
            slotCnt++;
        }

        //decrease the usedptr pointer value
        usedPtr = usedPtr - recLen;
        return OK;
    }
    else
        return DONE;
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID &rid)
{
    // fill in the body
    if (rid.pageNo == curPage && rid.slotNo >= 0)
    {
        int offset_memadd = slot[rid.slotNo].offset;
        int mem_len = slot[rid.slotNo].length;
        int slot_number = rid.slotNo;
        if (rid.slotNo < slotCnt)
        {
            slot[rid.slotNo].offset = -1;
            slot[rid.slotNo].length = 0;
            //delete the entry in the slot and the memory
            //update the slot arrays with the new address
            for (int i = 0; i < (slotCnt); i++)
            {
                if (slot[i].offset != -1)
                {
                    if (offset_memadd > slot[i].offset)
                        slot[i].offset = slot[i].offset + mem_len;
                }
            }
            //destination to copy
            int dest = offset_memadd;
            //source to copy from
            int source = dest - mem_len;
            //number of bytes to be copied
            int no_bytes = source - usedPtr;
            //compact the memory
            memmove(&data[usedPtr + mem_len], &data[usedPtr], no_bytes * sizeof(char));
            usedPtr = usedPtr + mem_len;
            if (rid.slotNo == (slotCnt - 1))
            {
                //delete the slot
                slotCnt = slotCnt - 1;
                //check if this current slot is empty if yes than delete it
                while (slot[(slotCnt - 1)].offset == -1)
                {
                    slotCnt = slotCnt - 1;
                }
            }

            return OK;
        }
        else
        {
            return FAIL;
        }
        /*
          int offset_memadd = slot[rid.slotNo].offset;
        int mem_len = slot[rid.slotNo].length;
        int slot_number = rid.slotNo;
        if (rid.slotNo < (slotCnt - 1))
        {
           
            slot[rid.slotNo].offset = -1;
            slot[rid.slotNo].length = 0;
            //delete the entry in the slot and the memory
            //update the slot arrays with the new address
            for (int i = rid.slotNo + 1; i < (slotCnt); i++)
            {
                if (slot[i].offset != -1)
                {
                    slot[i].offset = slot[i].offset + mem_len;
                }
            }
            //destination to copy
            int dest = offset_memadd;
            //source to copy from
            int source = dest - mem_len;
            //number of bytes to be copied
            int no_bytes = source - usedPtr;
            //compact the memory
            memmove(&data[usedPtr + mem_len], &data[usedPtr], no_bytes * sizeof(char));
            usedPtr = usedPtr + mem_len;
            return OK;
        }
        else if (rid.slotNo == (slotCnt - 1))
        {
            //delete the slot
            slotCnt = slotCnt - 1;
            usedPtr = usedPtr + mem_len;
      
           
                //check if this current slot is empty if yes than delete it
                while (slot[(slotCnt - 1)].offset == -1)
                {
                   
                    slotCnt = slotCnt - 1;
                }

            return OK;
        }
        else
        {
            return FAIL;
        }

  
        */
        //usedptr modify
    }

    return FAIL;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID &firstRid)
{
    // fill in the body
    if ((firstRid.pageNo != curPage))
    {
        return FAIL;
    }

    for (int i = 0; i < slotCnt; i++)
    {
        //check for record offset that is not -1
        if (slot[i].offset != -1)
        {
            firstRid.slotNo = i;
            firstRid.pageNo = curPage;
            return OK;
        }
    }

    return DONE;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord(RID curRid, RID &nextRid)
{
    // fill in the body
    if ((curRid.slotNo < 0) | (curRid.slotNo > slotCnt) | (curRid.pageNo != curPage))
    {
        return FAIL;
    }
    for (int i = curRid.slotNo + 1; i < slotCnt; i++)
    {
        if (slot[i].offset != -1)
        {
            nextRid.slotNo = i;
            nextRid.pageNo = curPage;
            return OK;
        }
    }
    return DONE;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char *recPtr, int &recLen)
{
    // fill in the body
    if ((rid.slotNo < 0) | (rid.slotNo > slotCnt) | (rid.pageNo != curPage))
    {
        return FAIL;
    }
    recLen = slot[rid.slotNo].length;
    memcpy(&recPtr, &data[slot[rid.slotNo].offset /*+ sizeof(slot_t) * rid.slotNo*/ - recLen], recLen * sizeof(char));
    //  int offset_ptr=(slot[rid.slotNo].offset + sizeof(slot_t)*rid.slotNo);
    //  *recPtr=data[offset_ptr-slot[rid.slotNo].length];
    //  *recPtr = data[(slot[rid.slotNo].offset + sizeof(slot_t)*rid.slotNo)];
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char *&recPtr, int &recLen)
{
    // fill in the body

    if ((rid.slotNo < 0) | (rid.slotNo > slotCnt) | (rid.pageNo != curPage))
    {
        return FAIL;
    }

    recLen = slot[rid.slotNo].length;
    recPtr = &data[slot[rid.slotNo].offset /* + sizeof(slot_t) * rid.slotNo*/ - recLen];
    //   memcpy( recPtr,&data[usedPtr-recLen], slot[rid.slotNo].length * sizeof(char));
    // recPtr = &data[(slot[rid.slotNo].offset + sizeof(slot_t)*rid.slotNo)];

    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    // fill in the body
    //check the Number of empty slots
    freeSpace = usedPtr + sizeof(slot_t) * (1 - slotCnt);
    for (int i = 0; i < slotCnt; i++)
    {
        if (slot[i].offset == -1)
        {
            //if there is empty space than
            return freeSpace;
        }
    }

    return (freeSpace - (sizeof(slot_t)));
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    // fill in the body
    for (int i = 0; i < slotCnt; i++)
    {
        if (slot[i].offset != -1)
            return false;
    }
    return true;
}
