/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */

BTreeFileScan::~BTreeFileScan()
{
	// put your code here
}

Status BTreeFileScan::get_next(RID &rid, void *keyptr)
{
	char temp[MAX_KEY_SIZE1] = {'\0'};
	if (Attr_type == attrString)
		memset(keyptr, '\0', MAX_KEY_SIZE1);
	Page *l_page;
	BTLeafPage *t_leaf = new BTLeafPage;
	RID dataRid, new_rid;
	Status state;
	if (current_rid.pageNo == -1) //this is the 1st rid means the lowest
	{
		state = MINIBASE_BM->pinPage(low_key_Id, l_page, 0);
		memcpy(t_leaf, l_page, 1024);
		//loop till you find the low key value in that page
		//	Keytype *t_key = new Keytype();
		RID t_rid;
		//state = t_leaf->get_first(rid, keyptr, dataRid);
		state = t_leaf->get_first(current_rid, keyptr, rid);
		if (lo_key == NULL)
		{
			//current_rid = dataRid;
			delete t_leaf;
			state = MINIBASE_BM->unpinPage(low_key_Id, FALSE, FALSE);
			if (hi_key != NULL)
				if (keyCompare(hi_key, keyptr, Attr_type) < 0)
					return DONE;
			return OK;
		}
		while (state == OK)
		{
			int k1 = keyCompare(lo_key, keyptr, Attr_type);
			if (k1 <= 0)
			{
				//current_rid = rid;
				delete t_leaf;
				state = MINIBASE_BM->unpinPage(low_key_Id, FALSE, FALSE);
				if (hi_key != NULL)
					if (keyCompare(hi_key, keyptr, Attr_type) < 0)
						return DONE;
				return OK;
			}
			if (Attr_type == attrString)
				memset(keyptr, '\0', MAX_KEY_SIZE1);
			//state = t_leaf->get_next(rid, keyptr, dataRid);
		Status new_state = OK;
		state = t_leaf->get_next(current_rid, keyptr, rid);
		int cond = 0;
		new_rid = current_rid;
		new_state = state;
		while (cond == 0 && state == OK)
		{
			new_rid = current_rid;
			new_state = state;
			state = t_leaf->get_next(current_rid, temp, dataRid);
			cond = keyCompare(temp, keyptr, Attr_type);
			if (cond == 0)
			{
				rid = dataRid;
			}
		}
		state = new_state;
		current_rid = new_rid;
		}
		return DONE;
	}
	else
	{
		state = MINIBASE_BM->pinPage(current_rid.pageNo, l_page, 0);
		memcpy(t_leaf, l_page, 1024);
		//	state = t_leaf->get_next(rid, keyptr, dataRid);
		Status new_state = OK;
		state = t_leaf->get_next(current_rid, keyptr, rid);
		int cond = 0;
		new_rid = current_rid;
		new_state = state;
		while (cond == 0 && state == OK)
		{
			new_rid = current_rid;
			new_state = state;
			state = t_leaf->get_next(current_rid, temp, dataRid);
			cond = keyCompare(temp, keyptr, Attr_type);
			if (cond == 0)
			{
				rid = dataRid;
			}
		}
		state = new_state;
		current_rid = new_rid;
		if (state == OK)
		{
			//	current_rid = rid;
			delete t_leaf;
			state = MINIBASE_BM->unpinPage(current_rid.pageNo, FALSE, FALSE);
			if (hi_key != NULL)
				if (keyCompare(hi_key, keyptr, Attr_type) < 0)
					return DONE;
			return OK;
		}
		else
		{
			//get the next page
			if (t_leaf->getNextPage() == -1)
			{
				delete t_leaf;
				state = MINIBASE_BM->unpinPage(current_rid.pageNo, FALSE, FALSE);
				return DONE;
			}
			else
			{
				state = MINIBASE_BM->unpinPage(current_rid.pageNo, FALSE, FALSE);
				state = MINIBASE_BM->pinPage(t_leaf->getNextPage(), l_page, 0);
				memcpy(t_leaf, l_page, 1024);
				//state = t_leaf->get_first(rid, keyptr, dataRid);
				state = t_leaf->get_first(current_rid, keyptr, rid);
				//	current_rid = rid;
				delete t_leaf;
				state = MINIBASE_BM->unpinPage(current_rid.pageNo, FALSE, FALSE);
				if (hi_key != NULL)
					if (keyCompare(hi_key, keyptr, Attr_type) < 0)
						return DONE;
				return OK;
			}
		}
	}

	return OK;
}

Status BTreeFileScan::delete_current()
{
	// put your code here
	BTLeafPage *t_leaf = new BTLeafPage;
	Page *l_page;
	Keytype *t_key = new Keytype();
	RID nextRid, dataRid, rid;
	Status state = MINIBASE_BM->pinPage(current_rid.pageNo, l_page, 0);
	memcpy(t_leaf, l_page, 1024);
	state = t_leaf->HFPage::nextRecord(current_rid, nextRid);
//	cout<<"curr rid "<<current_rid.pageNo<<" "<<current_rid.slotNo<<endl;
	if (state == OK)
	{
		state = t_leaf->HFPage::deleteRecord(current_rid);
	//	current_rid = nextRid;
		memcpy(l_page, t_leaf, 1024);
		delete t_leaf;
		state = MINIBASE_BM->unpinPage(current_rid.pageNo, 1, FALSE);
		return state;
	}
	else
	{
		PageId t_pageId = current_rid.pageNo;
		state = t_leaf->HFPage::deleteRecord(current_rid);
		if (t_leaf->getNextPage() == -1)
		{
			delete t_leaf;
			state = MINIBASE_BM->unpinPage(t_pageId, 1, FALSE);
			return DONE;
		}
		else
		{
			state = MINIBASE_BM->unpinPage(t_pageId, 1, FALSE);
			low_key_Id = t_leaf->getNextPage();
			current_rid.pageNo == -1;
			delete t_leaf;
			delete t_key;
			return OK;
			state = MINIBASE_BM->pinPage(t_leaf->getNextPage(), l_page, 0);
			memcpy(t_leaf, l_page, 1024);
			state = t_leaf->get_first(rid, t_key, dataRid);
			current_rid = rid;
			delete t_leaf;
			state = MINIBASE_BM->unpinPage(current_rid.pageNo, FALSE, FALSE);
			return OK;
		}
	}
}

int BTreeFileScan::keysize()
{
	// put your code here
	if (Attr_type == attrInteger)
		return sizeof(int);
	else if (Attr_type == attrString)
		return MAX_KEY_SIZE1;
	else
		return -1;
}
