/*
 * btleaf_page.C - implementation of class BTLeafPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */
#include <memory.h>
#include "btleaf_page.h"

const char *BTLeafErrorMsgs[] = {
    // OK,
    // Insert Record Failed,
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);

/*
 * Status BTLeafPage::insertRec(const void *key,
 *                             AttrType key_type,
 *                             RID dataRid,
 *                             RID& rid)
 *
 * Inserts a key, rid value into the leaf node. This is
 * accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call
 * to SortedPage::insertRecord() 
 * 
 * Parameters:
 *   o key - the key value of the data record.
 *
 *   o key_type - the type of the key.
 * 
 *   o dataRid - the rid of the data record. This is
 *               stored on the leaf page along with the
 *               corresponding key value.
 *
 *   o rid - the rid of the inserted leaf record data entry.
 */

Status BTLeafPage::insertRec(const void *key,
                             AttrType key_type,
                             RID dataRid,
                             RID &rid)
{
  // put your code here
  RID t_rid = dataRid;
  int recLen = 0;
  struct KeyDataEntry *t_target = new KeyDataEntry();
  Datatype t_Datatype;
  t_Datatype.rid = dataRid;
  make_entry(t_target, key_type, key, LEAF, t_Datatype, &recLen);
  Status s = get_data_rid((void *)key, key_type, t_rid);
  if (s  != OK)
  {
    Status state = SortedPage::insertRecord(key_type, (char *)t_target, recLen, rid);
    //delete data;
    return state;
  }
  else
  {
    return OK;
  }
}

/*
 *
 * Status BTLeafPage::get_data_rid(const void *key,
 *                                 AttrType key_type,
 *                                 RID & dataRid)
 *
 * This function performs a binary search to look for the
 * rid of the data record. (dataRid contains the RID of
 * the DATA record, NOT the rid of the data entry!)
 */

Status BTLeafPage::get_data_rid(void *key,
                                AttrType key_type,
                                RID &dataRid)
{
  // put your code here
  Keytype *t_key = new Keytype();
  RID t_rid;

  Status state = get_first(t_rid, t_key, dataRid);
  if (state != OK)
    return FAIL;
  PageId past_Id, next_Id;
  if (key == NULL)
  {
    return OK;
  }
  while (state == OK)
  {
    int k1 = keyCompare(key, t_key, key_type);
    if (k1 == 0)
      return OK;
    if (key_type == attrString)
      memset(t_key->charkey, '\0', MAX_KEY_SIZE1);
    state = get_next(t_rid, t_key, dataRid);
  }
  return FAIL;
}

/* 
 * Status BTLeafPage::get_first (const void *key, RID & dataRid)
 * Status BTLeafPage::get_next (const void *key, RID & dataRid)
 * 
 * These functions provide an
 * iterator interface to the records on a BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.
 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid. 
 */
Status BTLeafPage::get_first(RID &rid,
                             void *key,
                             RID &dataRid)
{
  // put your code here
  int reclen = 0;
  Status s = HFPage::firstRecord(rid);
  if (s != OK)
    return FAIL;
  struct KeyDataEntry *kde = new KeyDataEntry();
  HFPage::getRecord(rid, (char *)kde, reclen);
  Datatype t_Datatype;
  get_key_data(key, &t_Datatype, kde, reclen, LEAF);
  dataRid = t_Datatype.rid;
  //void get_key_data(void *targetkey, Datatype *targetdata,
  //                KeyDataEntry *psource, int entry_len, nodetype ndtype)
  return OK;
}

Status BTLeafPage::get_next(RID &rid,
                            void *key,
                            RID &dataRid)
{
  // put your code here
  RID nextRid;
  int reclen = 0;
  Status state = HFPage::nextRecord(rid, nextRid);
  if (state == DONE)
  {
    return DONE;
  }
  struct KeyDataEntry *kde = new KeyDataEntry();
  HFPage::getRecord(nextRid, (char *)kde, reclen);
  Datatype t_Datatype;
  get_key_data(key, &t_Datatype, kde, reclen, LEAF);
  dataRid = t_Datatype.rid;
  rid = nextRid;
  return OK;
}
