/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */
#include <memory.h>
#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"

const char *SortedPage::Errors[SortedPage::NR_ERRORS] = {
	//OK,
	//Insert Record Failed (SortedPage::insertRecord),
	//Delete Record Failed (SortedPage::deleteRecord,
};

/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */

Status SortedPage::insertRecord(AttrType key_type,
								char *recPtr,
								int recLen,
								RID &rid)
{
	// put your code here
	Status state = HFPage::insertRecord(recPtr, recLen, rid);
	//use the rid obtained for comparision
	int page_No = rid.pageNo;
	// Keytype;

	if (state != OK)
		return FAIL;
	int i = 0;
	for (; i < slotCnt; i++)
	{
		//find an empty slot with -1 offset
		if (slot[i].offset == -1)
		{
			//ignore
		}
		else if (i == rid.slotNo)
		{
		}
		else
		{
			int comp_val;
			//compare the keys of the record pointed out by the key..If it gives positive than there is problem
			if (key_type == attrString)
			{
				char rec[200] = {""};
				char temp_rec[200] = {""};
				char temp_recptr[200] = {""};
				memcpy(rec, &data[slot[i].offset - slot[i].length], (sizeof(char) * slot[i].length));
				memcpy(temp_rec, &data[slot[i].offset - slot[i].length], strlen(rec));
				memcpy(temp_recptr, recPtr, strlen(recPtr));
				//cout << "Comparing  " << recPtr << " reclen " << recLen << " by str " << strlen(rec) << " | " << rec << " length rec" << strlen(rec) << endl;
				comp_val = keyCompare(temp_recptr, temp_rec, key_type);
			}
			else
			{
				comp_val = keyCompare(recPtr, &data[slot[i].offset - recLen], key_type);
			}

			if (comp_val == 0)
			{
				if (i == rid.slotNo)
					return OK;
				else
					break;
			}
			if (comp_val < 0)
				break; //found the rid, will need to change the RID to there
		}
	}
	if (i == slotCnt)
	{
		return OK;
	}
	int temp_offset, temp_length;
	temp_offset = slot[rid.slotNo].offset;
	temp_length = slot[rid.slotNo].length;
	//start from the back of the slot before the current slot that is lesser and move it till we reach the desired slot
	int j = rid.slotNo;
	if (j > i)
	{
		for (; j > i; j--)
		{
			slot[j].offset = slot[j - 1].offset;
			slot[j].length = slot[j - 1].length;
			if (j - 1 < 0)
			{
				cout << "**********We HAVE reached undesireable condition in Sorted page" << endl;
			}
		}
	}
	else
	{
		for (; j < (i-1); j++)
		{
			slot[j].offset = slot[j + 1].offset;
			slot[j].length = slot[j + 1].length;
			if (j - 1 < 0)
			{
				cout << "**********We HAVE reached undesireable condition in Sorted page" << endl;
			}
		}
	}

	slot[j].offset = temp_offset;
	slot[j].length = temp_length;
	return OK;
}

/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord(const RID &rid)
{
	// put your code here
	Status state = HFPage::deleteRecord(rid);
	return state;
}

int SortedPage::numberOfRecords()
{
	// put your code here
	int num_records = 0;
	for (int i = 0; i < slotCnt; i++)
	{
		if (slot[i].length != -1)
		{
			num_records++;
		}
	}
	return num_records;
}
