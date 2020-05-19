
#include <string.h>
#include <assert.h>
#include "sortMerge.h"

// Error Protocall:

enum ErrCodes
{
	SORT_FAILED,
	HEAPFILE_FAILED
};

static const char *ErrMsgs[] = {
	"Error: Sort Failed.",
	"Error: HeapFile Failed."
	// maybe more ...
};

static error_string_table ErrTable(JOINS, ErrMsgs);

// sortMerge constructor
sortMerge::sortMerge(
	char *filename1,	  // Name of heapfile for relation R
	int len_in1,		  // # of columns in R.
	AttrType in1[],		  // Array containing field types of R.
	short t1_str_sizes[], // Array containing size of columns in R
	int join_col_in1,	 // The join column of R

	char *filename2,	  // Name of heapfile for relation S
	int len_in2,		  // # of columns in S.
	AttrType in2[],		  // Array containing field types of S.
	short t2_str_sizes[], // Array containing size of columns in S
	int join_col_in2,	 // The join column of S

	char *filename3,  // Name of heapfile for merged results
	int amt_of_mem,   // Number of pages available
	TupleOrder order, // Sorting order: Ascending or Descending
	Status &s		  // Status of constructor
)
{
	// fill in the body
	//cout << "In SortMerge " << endl;
	//Sort file 1
	char *t_file1 = "t_f1";

	Status state = OK;
	int num_of_buf = MINIBASE_BM->getNumUnpinnedBuffers();
	//cout << "Number of Unpinned Pages" << num_of_buf << endl;
	HeapFile *t_heap1 = new HeapFile(t_file1, state);
	//cout << "Status :" << state << endl;
	num_of_buf = MINIBASE_BM->getNumUnpinnedBuffers();
	//cout << "Number of Unpinned Pages" << num_of_buf << endl;
	Sort t_sort1(filename1, t_file1, len_in1, in1, t1_str_sizes, join_col_in1, order, num_of_buf, state);
	//cout << "Status :" << state << endl;
	//Sort file 2
	char *t_file2 = "t_f2";
	HeapFile *t_heap2 = new HeapFile(t_file2, state);
	//cout << "Status :" << state << endl;
	num_of_buf = MINIBASE_BM->getNumUnpinnedBuffers();
	//cout << "Number of Unpinned Pages" << num_of_buf << endl;
	Sort *t_sort2 = new Sort(filename2, t_file2, len_in2, in2, t2_str_sizes, join_col_in2, order, num_of_buf, state);
	//cout << "Status :" << state << endl;

	//Join file3
	HeapFile *t_join = new HeapFile(filename3, state);
	//cout << "Status :" << state << endl;

	//set both the key positions
	key1_pos = 0;
	for (int i = 0; i < join_col_in1; i++)
		key1_pos = key1_pos + t1_str_sizes[i];
	key1_pos = 0;
	for (int i = 0; i < join_col_in2; i++)
		key2_pos = key2_pos + t2_str_sizes[i];
	//set the rid for both the files R and S

	Scan *t_s1 = t_heap1->openScan(state);
	//cout << "Status :" << state << endl;
	Scan *t_s2 = t_heap2->openScan(state);
	//cout << "Status :" << state << endl;

	//Move the curson to 1st RID for both the files R and S
	RID f1_rid, f2_rid, s_rid,s1_old,s2_old;
	f1_rid.slotNo = -1;
	f2_rid.slotNo = -1;
	s = t_s1->position(f1_rid);
	//cout << "Status :" << s << endl;
	s = t_s2->position(f2_rid);
	//cout << "Status :" << s << endl;
	//cout << "Number of Records in R " << t_heap1->getRecCnt() << endl;
	//cout << "Number of Records in S " << t_heap2->getRecCnt() << endl;
	int r_len = 0;
	int s_len = 0;
	int t_out = 0;
	//Status s1 = OK;
	//Status s2 = OK;
	Status s1 = t_s1->mvNext(f1_rid);
	Status s2 = t_s2->mvNext(f2_rid);
	bool last_updated_s1;
	bool last_updated_s2;
	while ((s1 == OK) && (s2 == OK))
	{
		last_updated_s1=false;
		last_updated_s2=false;
		char r_data[200] = {""};
		char s_data[200] = {""};
		s = t_heap1->getRecord(f1_rid, r_data, r_len);
		//cout << "Status :" << s << endl;
		s = t_heap2->getRecord(f2_rid, s_data, s_len);
		//cout << "Status :" << s << endl;
		//do the tuple comparision
		t_out = tupleCmp(r_data, s_data);
		while (t_out < 0 && s1 != DONE)
		{
			s1 = t_s1->mvNext(f1_rid);
			//cout << "Status :" << s1 << endl;
			s = t_heap1->getRecord(f1_rid, r_data, r_len);
			//cout << "Status :" << s << endl;
			t_out = tupleCmp(r_data, s_data);
			//last_updated_s1=true;
		}
		while (t_out > 0 && s2 != DONE)
		{
			s2 = t_s2->mvNext(f2_rid);
			//cout << "Status :" << s2 << endl;
			s = t_heap2->getRecord(f2_rid, s_data, s_len);
			//cout << "Status :" << s << endl;
			t_out = tupleCmp(r_data, s_data);
			//last_updated_s2=true;
		}
		s_rid = f2_rid;
		while (t_out == 0)
		{
			
			s_rid = f2_rid;
			s = t_s2->position(s_rid);
			//cout << "Status :" << s << endl;
			s = t_heap2->getRecord(s_rid, s_data, r_len);
			//cout << "Status :" << s << endl;
			t_out = tupleCmp(r_data, s_data);
			while (t_out == 0)
			{
				//add the result to the join file
				char temp[100];
				RID join_rid;
				memcpy(temp, s_data, s_len);
				memcpy(temp + s_len, r_data, r_len);
				s = t_join->insertRecord(temp, s_len + r_len, join_rid);
			//	cout << "The Key Inserted is " << *(int *)s_data << endl;
			//	cout << "Status :" << s << endl;
		
				//s1 = t_s1->mvNext(f1_rid);
				//cout << "Status :" << s1 << endl;
				s2 = t_s2->mvNext(s_rid);
				//cout << "Status :" << s2 << endl;
				if(s2==DONE)
					break;

				//s = t_heap1->getRecord(f1_rid, r_data, r_len);
			
				//cout << "Status :" << s << endl;
				s = t_heap2->getRecord(s_rid, s_data, r_len);
				if(s==DONE)
					break;
				//cout << "Status :" << s << endl;
				t_out = tupleCmp(r_data, s_data);
			}
			//f2_rid = s_rid;
			s1 = t_s1->mvNext(f1_rid);
				if(s1==DONE)
					break;

			s = t_heap1->getRecord(f1_rid, r_data, r_len);
			if(s==DONE)
			break;
			s = t_heap2->getRecord(f2_rid, s_data, r_len);
			t_out = tupleCmp(r_data, s_data);
			if(t_out!=0)
				f2_rid = s_rid;
		}
	}
	delete t_heap1;
	delete t_heap2;
	s = MINIBASE_DB->delete_file_entry(t_file1);
	s = MINIBASE_DB->delete_file_entry(t_file2);
	//	delete t_sort1;
	//delete t_sort2;
	s = OK;
}

// sortMerge destructor
sortMerge::~sortMerge()
{
	// fill in the body
}
