/*
 * btfile.C - function members of class BTreeFile 
 * 
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

// Define your error message here
const char *BtreeErrorMsgs[] = {
	// Possible error messages
	// _OK
	// CANT_FIND_HEADER
	// CANT_PIN_HEADER,
	// CANT_ALLOC_HEADER
	// CANT_ADD_FILE_ENTRY
	// CANT_UNPIN_HEADER
	// CANT_PIN_PAGE
	// CANT_UNPIN_PAGE
	// INVALID_SCAN
	// SORTED_PAGE_DELETE_CURRENT_FAILED
	// CANT_DELETE_FILE_ENTRY
	// CANT_FREE_PAGE,
	// CANT_DELETE_SUBTREE,
	// KEY_TOO_LONG
	// INSERT_FAILED
	// COULD_NOT_CREATE_ROOT
	// DELETE_DATAENTRY_FAILED
	// DATA_ENTRY_NOT_FOUND
	// CANT_GET_PAGE_NO
	// CANT_ALLOCATE_NEW_PAGE
	// CANT_SPLIT_LEAF_PAGE
	// CANT_SPLIT_INDEX_PAGE
};

static error_string_table btree_table(BTREE, BtreeErrorMsgs);

BTreeFile::BTreeFile(Status &returnStatus, const char *filename)
{
	// put your code here
	if (MINIBASE_DB->get_file_entry(filename, header_pageID) == OK)
	{
		Status state;
		//cout << "The file already exists" << endl;
		Page *t_page;
		state = MINIBASE_BM->pinPage(header_pageID, global_page, 0, filename);
		memcpy(&head_page, global_page, sizeof(Head_page));
		returnStatus = state;
		memcpy(fname, filename, strlen(filename));
	}
	else
	{
		returnStatus = FAIL;
	}
}

BTreeFile::BTreeFile(Status &returnStatus, const char *filename,
					 const AttrType keytype,
					 const int keysize)
{
	// put your code here
	Status state;
	if (MINIBASE_DB->get_file_entry(filename, header_pageID) == OK)
	{
		//	cout << "The file already exists" << endl;
		state = MINIBASE_BM->pinPage(header_pageID, global_page, 0, filename);
		memcpy(&head_page, global_page, sizeof(Head_page));
		returnStatus = state;
		memcpy(fname, filename, strlen(filename));
	}
	else
	{
		//Now create a index
		if (keytype != attrInteger && keytype != attrString)
			returnStatus = FAIL;
		else
		{
			//		cout << "create the file" << endl;
			Page *h_page;
			Page *r_page;
			BTLeafPage *t_leaf = new BTLeafPage;
			state = MINIBASE_BM->newPage(header_pageID, global_page, 1);
			//		cout << "The header Page Number opened is " << header_pageID << endl;
			state = MINIBASE_DB->add_file_entry(filename, header_pageID);
			state = MINIBASE_DB->get_file_entry(filename, header_pageID);
			state = MINIBASE_BM->newPage(head_page.root_pageId, r_page, 1);
			memcpy(t_leaf, r_page, 1024);
			t_leaf->init(head_page.root_pageId);
			memcpy(r_page, t_leaf, 1024);
			//		cout << "The Root Page Number opened is " << head_page.root_pageId << endl;
			head_page.file_AttrType = keytype;
			memcpy(global_page, &head_page, sizeof(Head_page));

			state = MINIBASE_BM->unpinPage(head_page.root_pageId, TRUE, filename);
			level = 1;
			memcpy(fname, filename, strlen(filename));
			returnStatus = state;
			delete t_leaf;
		}
	}
}

BTreeFile::~BTreeFile()
{
	// put your code here
	if (destroyed == false)
	{
		memcpy(global_page, &head_page, sizeof(Head_page));
		Status state = MINIBASE_BM->unpinPage(header_pageID, TRUE, fname);
	}
}

Status BTreeFile::destroyFile()
{
	// put your code here
	int curr_level = 1;
	Page *i_page;
	Page *l_page;
	//bool leaf_found = false;
	Status state;
	PageId curr_pageId = head_page.root_pageId;

	char smallest_record[300] = "";
	char record[300] = "";
	BTLeafPage *t_leaf = new BTLeafPage;
	BTIndexPage *t_index = new BTIndexPage;
	Keytype *t_key = new Keytype();
	Keytype *f_key = new Keytype();
	RID t_rid;
	int pageId_transversed[5] = {-1, -1, -1, -1, -1}; //page No my there level
	level=2;
	while (1)
	{
		if (curr_level < level) //this is used to iterate the tree to the correct Leaf node
		{
			//BTIndexPage *t_index = new BTIndexPage;
			state = MINIBASE_BM->pinPage(curr_pageId, i_page, 0, fname);
			memcpy(t_index, i_page, 1024);
			PageId get_pageId = -1;
			state = t_index->get_first(t_rid, f_key, get_pageId);
			if (curr_level + 1 == level)
			{
				//start deleting all the leafs in the curr_pageId
				while (state == OK)
				{
					state = MINIBASE_BM->freePage(get_pageId);
					//				cout << "Page Free is " << get_pageId << " state " << state << endl;
					state = t_index->get_next(t_rid, t_key, get_pageId);
				}
				//delete that index page
				state = MINIBASE_BM->unpinPage(curr_pageId, 1, fname);
				state = MINIBASE_BM->freePage(curr_pageId);
				//			cout << "Page Free is " << curr_pageId << " state " << state << endl;
				//if the index page was root page than exit
				if (curr_pageId == head_page.root_pageId)
				{
					memcpy(global_page, &head_page, sizeof(Head_page));
					state = MINIBASE_DB->delete_file_entry(fname);
					Status state = MINIBASE_BM->unpinPage(header_pageID, TRUE, fname);
					state = MINIBASE_BM->freePage(header_pageID);
					//					cout << "Page Free is " << header_pageID << " state " << state << endl;
					delete t_leaf;
					delete t_index;
					delete t_key;
					delete f_key;
					return OK;
				}

				else
				{
					state = MINIBASE_BM->pinPage(pageId_transversed[curr_level - 2], i_page, 0, fname);
					memcpy(t_index, i_page, 1024);
					state = t_index->get_first(t_rid, f_key, get_pageId);
					state = t_index->deleteKey(f_key, head_page.file_AttrType, t_rid);
					memcpy(i_page, t_index, 1024);
					state = MINIBASE_BM->unpinPage(pageId_transversed[curr_level - 2], 1, fname);
				}
			}
			pageId_transversed[curr_level - 1] = curr_pageId;
			curr_pageId = get_pageId;
			state = MINIBASE_BM->unpinPage(head_page.root_pageId, 1, fname);
			//delete t_index;
		}
		curr_level++;
	}
	destroyed = true;
	return OK;
}

Status BTreeFile::insert(const void *key, const RID rid)
{
	// put your code here
	// if the level is 1 than there only leaf page at top
	int curr_level = 1;
	Page *i_page;
	Page *l_page;
	//bool leaf_found = false;
	Status state;
	PageId curr_pageId = head_page.root_pageId;
	int pageId_transversed[5] = {-1, -1, -1, -1, -1}; //page No my there level
	char smallest_record[300] = "";
	char record[300] = "";

	BTLeafPage *t_leaf = new BTLeafPage;
	BTLeafPage *new_t_leaf = new BTLeafPage;
	BTIndexPage *t_index = new BTIndexPage;
	BTLeafPage *temp_leaf = new BTLeafPage;
	KeyDataEntry *psource = new KeyDataEntry;

	while (curr_level <= level)
	{
		if (curr_level == level) // Now I am at leaf
		{
			//BTLeafPage *t_leaf = new BTLeafPage;
			state = MINIBASE_BM->pinPage(curr_pageId, l_page, 0, fname);
			memcpy(t_leaf, l_page, 1024);
			if (t_leaf->empty() == true)
			{
				t_leaf->init(curr_pageId);
			}
			/*
			if (t_leaf->available_space() < 12)
			{
				cout << "We stop here \n";
			}
			
			if (head_page.file_AttrType == attrInteger)
			{
			cout << "SPA " << t_leaf->available_space() << " Key " << *(int *)key << " RID Page " << rid.pageNo << " Slot " << rid.slotNo << endl;
			}
			else
			{
				cout << "SPA " << t_leaf->available_space() << " Key " << (char *)key << " RID Page " << rid.pageNo << " Slot " << rid.slotNo << endl;
			}
*/

			RID getrid;
			state = t_leaf->insertRec(key, head_page.file_AttrType, rid, getrid);
			if (state == OK)
			{
				memcpy(l_page, t_leaf, 1024);
				state = MINIBASE_BM->unpinPage(curr_pageId, 1, fname);
				delete temp_leaf;
				delete t_index;
				delete t_leaf;
				delete psource;
				delete new_t_leaf;
				return OK;
			}
			else
			{
				state = MINIBASE_BM->unpinPage(curr_pageId, 1, fname); //split
				//	delete t_leaf;

				//		BTLeafPage *t_leaf = new BTLeafPage;
				//****************this is for Leaf************//
				state = MINIBASE_BM->pinPage(curr_pageId, l_page, 0, fname);
				memcpy(t_leaf, l_page, 1024);
				//split the root node
				//if int the 1st 30 will goto the 1 node and the rest will be divided to the other node
				//BTLeafPage *new_t_leaf = new BTLeafPage;
				Page *new_page;
				//int entry_move = t_leaf->slotCnt / 2;
				int entry_move = t_leaf->numberOfRecords() / 2;
				//create a new page
				PageId new_leaf_pageId = -1;
				state = MINIBASE_BM->newPage(new_leaf_pageId, new_page, 1);
				memcpy(new_t_leaf, new_page, 1024);
				new_t_leaf->init(new_leaf_pageId);
				//	cout << "The New split Page Number opened is " << new_leaf_pageId << endl;
				/*
				if (new_t_leaf->empty() == true)
				{
					new_t_leaf->init(new_leaf_pageId);
				}*/
				//check if this con be limitation

				int rec_len = 0;
				int string_len = 0;
				RID old_rid;
				RID new_rid;
				old_rid.pageNo = t_leaf->page_no();
				int old_num_rec = t_leaf->numberOfRecords();
				for (int i = entry_move; i < old_num_rec; i++)
				{
					old_rid.slotNo = i;
					state = t_leaf->getRecord(old_rid, record, rec_len); //get record
					if (i == entry_move)
					{
						//	strncpy(smallest_record, record, rec_len);
						memset(smallest_record, '\0', 300);
						memcpy(smallest_record, record, rec_len);
						string_len = rec_len; /*
						if (head_page.file_AttrType == attrInteger)
							cout << "Record that is split is " << *(int *)smallest_record << endl;
						else
							cout << "Record that is split is " << smallest_record << endl;*/
					}
					state = new_t_leaf->HFPage::insertRecord(record, rec_len, new_rid); //insert record
				}
				for (int i = t_leaf->numberOfRecords() - 1; i >= entry_move; i--) //delete lower half records
				{
					old_rid.slotNo = i;
					state = t_leaf->getRecord(old_rid, record, rec_len); //get record
																		 //	cout <<"Key Delete "<<i << " - >  " << *(int *)record;
					state = t_leaf->deleteRecord(old_rid);				 //	cout << state << " ,";
				}
				//print both pages completely with the data
				old_rid.pageNo = t_leaf->page_no();
/*
				for (int i = 0; i < t_leaf->numberOfRecords(); i++) //delete lower half records
				{
					old_rid.slotNo = i;
					state = t_leaf->getRecord(old_rid, record, rec_len); //get record
					if (head_page.file_AttrType == attrInteger)
						cout << i << " - >  " << *(int *)record << endl;
					else
					{
						char temp[200] = "";
						Datatype t_Datatype;
						memcpy(psource, record, rec_len);
						get_key_data(temp, &t_Datatype, psource, rec_len, LEAF);
						//memcpy(temp, record, rec_len);
						cout << i << " - >  " << temp << " RID Page " << t_Datatype.rid.pageNo << " Slot " << t_Datatype.rid.slotNo << endl;
					}
				}
				old_rid.pageNo = new_t_leaf->page_no();
				for (int i = 0; i < new_t_leaf->numberOfRecords(); i++) //delete lower half records
				{
					old_rid.slotNo = i;
					state = new_t_leaf->getRecord(old_rid, record, rec_len); //get record
					if (head_page.file_AttrType == attrInteger)
						cout << i << " - >  " << *(int *)record << endl;
					else
					{
						char temp[200] = "";
						Datatype t_Datatype;
						memcpy(psource, record, rec_len);
						get_key_data(temp, &t_Datatype, psource, rec_len, LEAF);
						//memcpy(temp, record, rec_len);
						cout << i << " - >  " << temp << " RID Page " << t_Datatype.rid.pageNo << " Slot " << t_Datatype.rid.slotNo << endl;
					}
				}*/

				//set the sibling pointers on both the places
				PageId old_pageId = t_leaf->page_no();
				PageId new_pageId = new_t_leaf->page_no();
				if (t_leaf->getNextPage() != -1)
				{
					Page *t_page;
					//BTLeafPage *temp_leaf = new BTLeafPage;
					PageId n_page = t_leaf->getNextPage(); //this page siblings will change
					state = MINIBASE_BM->pinPage(n_page, t_page, 0, fname);
					memcpy(temp_leaf, t_page, 1024);
					temp_leaf->setPrevPage(new_pageId); //change prev and next for the siblings that are being shifted
					new_t_leaf->setNextPage(n_page);
					memcpy(t_page, temp_leaf, 1024);
					state = MINIBASE_BM->unpinPage(n_page, 1, fname);
					//delete temp_leaf;
				}
				t_leaf->setNextPage(new_pageId);
				new_t_leaf->setPrevPage(old_pageId);
				memcpy(l_page, t_leaf, 1024);
				memcpy(new_page, new_t_leaf, 1024);

				if (old_pageId == head_page.root_pageId) //its a root and needs to be split
				{
					Page *r_page;
					state = MINIBASE_BM->newPage(head_page.root_pageId, r_page, 1);
					memcpy(t_index, r_page, 1024);
					//	cout << "The New Root Page Number opened is " << head_page.root_pageId << endl;
					t_index->init(head_page.root_pageId);
					RID getrid;

					if (head_page.file_AttrType == attrInteger)
					{
						int t_key = -1;
						state = t_index->insertKey(&t_key, head_page.file_AttrType, old_pageId, getrid);
						memcpy(&(psource->key.intkey), smallest_record, sizeof(int));
						state = t_index->insertKey(&(psource->key.intkey), head_page.file_AttrType, new_pageId, getrid);
						//		cout << "**************ENTRY PUSHED UP--->In root split*************** 		" << *(int *)smallest_record << endl;
					}
					else
					{

						char temp1[] = " ";
						state = t_index->insertKey(temp1, head_page.file_AttrType, old_pageId, getrid);
						char temp[200] = "";
						Datatype t_Datatype;
						memcpy(psource, smallest_record, string_len);
						get_key_data(temp, &t_Datatype, psource, string_len, LEAF);
						state = t_index->insertKey(temp, head_page.file_AttrType, new_pageId, getrid);
						//		cout << "**************ENTRY PUSHED UP--->In root split*************** 		" << temp << endl;
					}

					memcpy(r_page, t_index, 1024);
					state = MINIBASE_BM->unpinPage(head_page.root_pageId, 1, fname);
					level++;
				}
				else //pageId_transversed[curr_level - 2] check if there is empty if insert passes this than just add it to that index and forget it
				{
					Page *u_page;
					PageId upper_pageId = pageId_transversed[curr_level - 2];
					state = MINIBASE_BM->pinPage(upper_pageId, u_page, 0, fname);
					memcpy(t_index, u_page, 1024);
					if (t_index->empty() == true)
					{
						//		cout << "*******************THIS SHOULDNT HAPPEN BTFILE.C*********************" << endl;
					}
					if (head_page.file_AttrType == attrInteger)
					{
						memcpy(&(psource->key.intkey), smallest_record, sizeof(int));
						//		cout << "**************ENTRY PUSHED UP--->In Index*************** 		" << *(int *)smallest_record << "  Space Available " << t_index->available_space() << endl;
						state = t_index->insertKey(&(psource->key.intkey), head_page.file_AttrType, new_pageId, getrid);
					}
					else
					{
						char temp[200] = "";
						Datatype t_Datatype;
						memcpy(psource, smallest_record, string_len);
						get_key_data(temp, &t_Datatype, psource, string_len, LEAF);
						state = t_index->insertKey(temp, head_page.file_AttrType, new_pageId, getrid);
						//		cout << "**************ENTRY PUSHED UP--->Index*************** 		" << temp << endl;
					}

					memcpy(u_page, t_index, 1024);
					state = MINIBASE_BM->unpinPage(upper_pageId, 1, fname);
				}
				state = MINIBASE_BM->unpinPage(old_pageId, 1, fname);
				state = MINIBASE_BM->unpinPage(new_pageId, 1, fname);
				curr_level = 1;
				curr_pageId = head_page.root_pageId; //reset the curr_pageId
			}
		}
		if (curr_level < level) //this is used to iterate the tree to the correct Leaf node
		{
			//BTIndexPage *t_index = new BTIndexPage;
			state = MINIBASE_BM->pinPage(curr_pageId, i_page, 0, fname);
			memcpy(t_index, i_page, 1024);
			PageId get_pageId = -1;
			state = t_index->get_page_no(key, head_page.file_AttrType, get_pageId);
			pageId_transversed[curr_level - 1] = curr_pageId;
			curr_pageId = get_pageId;
			state = MINIBASE_BM->unpinPage(head_page.root_pageId, 1, fname);
			//delete t_index;
		}
		curr_level++;
	}

	return OK;
}

Status BTreeFile::Delete(const void *key, const RID rid)
{
	// put your code here
	int curr_level = 1;
	Page *i_page;
	Page *l_page;
	//bool leaf_found = false;
	Status state;
	PageId curr_pageId = head_page.root_pageId;
	int pageId_transversed[5] = {-1, -1, -1, -1, -1}; //page No my there level
	char smallest_record[300] = "";
	char record[300] = "";
	int rec_len = 0;
	BTLeafPage *t_leaf = new BTLeafPage;
	BTLeafPage *new_t_leaf = new BTLeafPage;
	BTIndexPage *t_index = new BTIndexPage;
	BTLeafPage *temp_leaf = new BTLeafPage;
	KeyDataEntry *psource = new KeyDataEntry;
	while (curr_level <= level)
	{
		if (curr_level == level) // Now I am at leaf
		{
			//BTLeafPage *t_leaf = new BTLeafPage;
			state = MINIBASE_BM->pinPage(curr_pageId, l_page, 0, fname);
			memcpy(t_leaf, l_page, 1024);
			if (t_leaf->empty() == true)
			{
				//			cout << "We got a problem here \n";
			}
			RID old_rid;
			old_rid.pageNo = t_leaf->page_no();
			int total_no_time = t_leaf->numberOfRecords();
/*
			for (int i = 0; i < total_no_time; i++) //delete lower half records
			{
				old_rid.slotNo = i;
				state = t_leaf->getRecord(old_rid, record, rec_len); //get record
				if (state == OK)
				{
					if (head_page.file_AttrType == attrInteger)
					{
						cout << i << " -> " << *(int *)record << "	";
					}
					else
					{
						char temp[200] = "";
						Datatype t_Datatype;
						memcpy(psource, record, rec_len);
						get_key_data(temp, &t_Datatype, psource, rec_len, LEAF);
						//memcpy(temp, record, rec_len);
						cout << i << " - >  " << temp << " RID Page " << t_Datatype.rid.pageNo << " Slot " << t_Datatype.rid.slotNo << endl;
					}

					if (i % 5 == 0)
						cout << endl;
				}
				else
					total_no_time++;
			}
			if (head_page.file_AttrType == attrInteger)
				cout << "Page No : " << curr_pageId << " Value " << *(int *)key << endl;
			else
				cout << "Page No : " << curr_pageId << " Value " << (char *)key << endl;
			cout << endl;*/
			//loop till you find the low key value in that page
			Keytype *t_key = new Keytype();
			RID t_rid;
			RID dataRid;
			state = t_leaf->get_first(t_rid, t_key, dataRid);
			bool first = true;
			while (state == OK)
			{
				int k1 = keyCompare(t_key, key, head_page.file_AttrType);
				if (k1 == 0)
				{
					/*
					if (dataRid.pageNo == rid.pageNo)
					//	cout << "pageNo matches";
					else
					//	cout << "pageNo donot matches";
					if (dataRid.slotNo == rid.slotNo)
					//	cout << "Slot No matches";
					else
					//	cout << "Slot No donot matches";
					cout << endl;*/
					state = t_leaf->deleteRecord(t_rid);
					if (first == true)
					{
						//push into Index that is upper level
						state = MINIBASE_BM->pinPage(pageId_transversed[curr_level - 2], i_page, 0, fname);
						memcpy(t_index, i_page, 1024);
						state = t_index->deleteKey(key, head_page.file_AttrType, dataRid);
						if (head_page.file_AttrType == attrString)
							memset(t_key->charkey, '\0', MAX_KEY_SIZE1);
						state = t_leaf->get_next(t_rid, t_key, dataRid);
						if (state == OK)
							state = t_index->insertKey(t_key, head_page.file_AttrType, curr_pageId, dataRid);
						/*	else
							cout << "Write code to delete it" << endl;*/
						memcpy(i_page, t_index, 1024);
						state = MINIBASE_BM->unpinPage(pageId_transversed[curr_level - 2], 1, fname);
					}
					break;
				}
				if (head_page.file_AttrType == attrString)
					memset(t_key->charkey, '\0', MAX_KEY_SIZE1);
				state = t_leaf->get_next(t_rid, t_key, dataRid);
				first = false;
			}
			delete t_key;
			if (state == OK)
			{
				memcpy(l_page, t_leaf, 1024);
				state = MINIBASE_BM->unpinPage(curr_pageId, 1, fname);
				delete temp_leaf;
				delete t_index;
				delete t_leaf;
				delete psource;
				delete new_t_leaf;
				return OK;
			}
			else
			{
				//	cout << "*******We got a problem in Delete***********" << endl;
				return FAIL;
			}
		}
		if (curr_level < level) //this is used to iterate the tree to the correct Leaf node
		{
			//BTIndexPage *t_index = new BTIndexPage;
			state = MINIBASE_BM->pinPage(curr_pageId, i_page, 0, fname);
			memcpy(t_index, i_page, 1024);
			PageId get_pageId = -1;
			state = t_index->get_page_no(key, head_page.file_AttrType, get_pageId);
			pageId_transversed[curr_level - 1] = curr_pageId;
			curr_pageId = get_pageId;
			state = MINIBASE_BM->unpinPage(head_page.root_pageId, 1, fname);
			//delete t_index;
		}
		curr_level++;
	}
	return OK;
}

IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key)
{
	// put your code here
	BTreeFileScan *t_scan = new BTreeFileScan;
	t_scan->lo_key = lo_key;
	t_scan->hi_key = hi_key;
	int k1 = 1;
	t_scan->current_rid.pageNo = -1;
	t_scan->Attr_type = head_page.file_AttrType;
	memcpy(t_scan->fname, fname, strlen(fname));
	if (lo_key == NULL && hi_key == NULL)
	{
		if (head_page.file_AttrType == attrInteger)
		{
			int low_key = -2147483647;
			int high_key = 2147483647;
		}
		else
		{
		}
	}
	else if (lo_key == NULL && hi_key != NULL)
	{
		if (head_page.file_AttrType == attrInteger)
		{
			int low_key = -2147483647;
			int high_key = *(int *)hi_key;
		}
	}
	else if (lo_key != NULL && hi_key == NULL)
	{
		if (head_page.file_AttrType == attrInteger)
		{
			int low_key = *(int *)lo_key;
			int high_key = 2147483647;
		}
	}
	else if (lo_key != NULL && hi_key != NULL)
	{
		k1 = keyCompare(lo_key, hi_key, head_page.file_AttrType);
		if (k1 == 0) //exact match
		{
			int low_key = *(int *)lo_key;
		}
		else
		{
			int low_key = *(int *)lo_key;
			int high_key = *(int *)hi_key;
		}
	}
	int curr_level = 1;
	Page *i_page;
	Page *l_page;
	//bool leaf_found = false;
	Status state;
	PageId curr_pageId = head_page.root_pageId;
	BTLeafPage *t_leaf = new BTLeafPage;
	BTIndexPage *t_index = new BTIndexPage;
	//low
	state = MINIBASE_BM->pinPage(curr_pageId, i_page, 0, fname);
	memcpy(t_index, i_page, 1024);
	PageId get_pageId = -1;
	state = t_index->get_page_no(lo_key, head_page.file_AttrType, get_pageId);
	curr_pageId = get_pageId;
	state = MINIBASE_BM->unpinPage(head_page.root_pageId, 1, fname);

	t_scan->low_key_Id = curr_pageId;

	delete t_index;
	delete t_leaf;
	return t_scan;
}

int keysize()
{
	// put your code here
	return 0;
}
