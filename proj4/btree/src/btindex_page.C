/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */
#include <memory.h>
#include "btindex_page.h"

// Define your Error Messge here
const char *BTIndexErrorMsgs[] = {
	//Possbile error messages,
	//OK,
	//Record Insertion Failure,
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

Status BTIndexPage::insertKey(const void *key,
							  AttrType key_type,
							  PageId pageNo,
							  RID &rid)
{
	// put your code here
	int recLen = 0;
	struct KeyDataEntry *t_target = new KeyDataEntry();
	Datatype t_Datatype;
	t_Datatype.pageNo = pageNo;
	make_entry(t_target, key_type, key, INDEX, t_Datatype, &recLen);
	Status state = SortedPage::insertRecord(key_type, (char *)t_target, recLen, rid);
	delete t_target;
	return state;
}

Status BTIndexPage::deleteKey(const void *key, AttrType key_type, RID &curRid)
{
	// put your code here
	Keytype *t_key = new Keytype();
	RID t_rid;
	PageId pageNo = -1;
	Status state = get_first(t_rid, t_key, pageNo);
	while (state == OK)
	{
		int k1 = keyCompare(key, t_key, key_type);
		if (k1 == 0)
		{
			HFPage::deleteRecord(t_rid);
			curRid = t_rid;
			return OK;
		}
		if (key_type == attrString)
			memset(t_key->charkey, '\0', MAX_KEY_SIZE1);
		state = get_next(t_rid, t_key, pageNo);
	}
	return FAIL;
}

Status BTIndexPage::get_page_no(const void *key,
								AttrType key_type,
								PageId &pageNo)
{
	// put your code here
	Keytype *t_key = new Keytype();
	RID t_rid;
	PageId past_Id, next_Id;
	Status state = get_first(t_rid, t_key, past_Id);
	if (key == NULL)
	{
		pageNo = past_Id;
		return OK;
	}
	int k1 = 0;
	int k2 = 0;
	bool first_time = true;
	while (state == OK)
	{
		if (key_type == attrString)
			memset(t_key->charkey, '\0', MAX_KEY_SIZE1);
		state = get_next(t_rid, t_key, next_Id);
		if (state == DONE)
		{
			pageNo = next_Id;
			return OK;
		}
		k1 = keyCompare(key, t_key, key_type);
		if (k1 < 0 && first_time == true)
		{
			pageNo = past_Id;
			return OK;
		}
		if (k2 >= 0 && k1 < 0 && first_time == false)
		{
			pageNo = past_Id;
			return OK;
		}
		first_time = false;
		k2 = k1;
		past_Id = next_Id;
	}
	return FAIL;
}

Status BTIndexPage::get_first(RID &rid,
							  void *key,
							  PageId &pageNo)
{
	// put your code here
	int reclen = 0;
	HFPage::firstRecord(rid);
	struct KeyDataEntry *kde = new KeyDataEntry();
	HFPage::getRecord(rid, (char *)kde, reclen);
	Datatype t_Datatype;
	get_key_data(key, &t_Datatype, kde, reclen, INDEX);
	pageNo = t_Datatype.pageNo;
	return OK;
}

Status BTIndexPage::get_next(RID &rid, void *key, PageId &pageNo)
{
	// put your code here
	RID nextRid;
	int reclen = 0;
	Status state;
	state = HFPage::nextRecord(rid, nextRid);
	if (state == DONE)
	{
		return DONE;
	}
	struct KeyDataEntry *kde = new KeyDataEntry();
	HFPage::getRecord(nextRid, (char *)kde, reclen);
	Datatype t_Datatype;
	get_key_data(key, &t_Datatype, kde, reclen, INDEX);
	pageNo = t_Datatype.pageNo;
	rid = nextRid;
	return OK;
}
