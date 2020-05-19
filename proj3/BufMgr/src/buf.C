/*****************************************************************************/
/*************** Implementation of the Buffer Manager Layer ******************/
/*****************************************************************************/

#include "buf.h"

// Define buffer manager error messages here
//enum bufErrCodes  {...};

// Define error message here
static const char *bufErrMsgs[] = {
    // error message strings go here
    "Not enough memory to allocate hash entry",
    "Inserting a duplicate entry in the hash table",
    "Removing a non-existing entry from the hash table",
    "Page not in hash table",
    "Not enough memory to allocate queue node",
    "Poping an empty queue",
    "OOOOOOPS, something is wrong",
    "Buffer pool full",
    "Not enough memory in buffer manager",
    "Page not in buffer pool",
    "Unpinning an unpinned page",
    "Freeing a pinned page"};

// Create a static "error_string_table" object and register the error messages
// with minibase system
static error_string_table bufTable(BUFMGR, bufErrMsgs);

//*************************************************************
//** This is the implementation of BufMgr
//************************************************************

BufMgr::BufMgr(int numbuf, Replacer *replacer)
{
  // put your code here
  numBuffers = numbuf;
  bufPool = new Page[numbuf];
  frmeTable = new FrameDesc[numbuf];
  hashTable = (struct hash_table *)calloc(HTSIZE, sizeof(struct hash_table));
  //initialize the pages in hastable to invalid
  for (int i = 0; i < HTSIZE; i++)
  {
    hashTable[i].page_number = INVALID_PAGE;
    hashTable[i].frame_number = INVALID_PAGE;
    hashTable[i].next = NULL;
  }

  //put all the pages in the hate list
  MRU_hated_head = NULL;
  LRU_loved_head = NULL;

  struct MRU_hated *t_MRU = MRU_hated_head;

  for (unsigned int i = 0; i < numBuffers; i++)
  {
    if (t_MRU == NULL)
    {
      MRU_hated_head = new MRU_hated;
      t_MRU = MRU_hated_head;
      t_MRU->next = NULL;
      t_MRU->frame_number = i;
    }
    else
    {
      t_MRU->next = new MRU_hated;
      t_MRU = t_MRU->next;
      t_MRU->next = NULL;
      t_MRU->frame_number = i;
    }
  }
}

//*************************************************************
//** This is the implementation of ~BufMgr
//************************************************************
BufMgr::~BufMgr()
{
  // put your code here
  //flush all the dirty page

  //clean the files
  //first clean the LRU and MRU Table
  struct MRU_hated *t_MRU = MRU_hated_head;
  struct MRU_hated *prev_MRU = NULL;
  for (; t_MRU != NULL;)
  {
    prev_MRU = t_MRU;
    t_MRU = t_MRU->next;
    delete prev_MRU;
  }
  MRU_hated_head = NULL;

  struct LRU_loved *t_LRU = LRU_loved_head;
  struct LRU_loved *prev_LRU = NULL;
  for (; t_LRU != NULL;)
  {
    prev_LRU = t_LRU;
    t_LRU = t_LRU->next;
    delete t_LRU;
  }
  LRU_loved_head = NULL;

  //Now clean the hash table
  hash_table *t_htable, *prev_table;
  for (int i = 0; i < HTSIZE; i++)
  {
    t_htable = &hashTable[i];
    t_htable = t_htable->next;
    while (t_htable != NULL)
    {
      prev_table = t_htable;
      t_htable = t_htable->next;
      delete t_htable;
    }
    hashTable[i].next = NULL;
  }
  //Now clean the frametable
  for (int i = 0; i < numBuffers; i++)
  {
    if (frmeTable[i].r_dirty_bit() == true)
    {
      Status state = MINIBASE_DB->write_page(frmeTable[i].pageNo, &bufPool[i]);
    }
  }
  delete[] frmeTable;
  delete[] bufPool;
}

//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage)
{
  Page *t_page;
  t_page=new Page;
  if (MINIBASE_DB->read_page(PageId_in_a_DB, t_page) != OK )
  {
    return FAIL;
  }
  delete t_page;
  int hvalue = (PageId_in_a_DB) % HTSIZE;
  hash_table *t_htable = &hashTable[hvalue];
  hash_table *prev_htable;

  for (; t_htable != NULL; t_htable = t_htable->next)
  {
    //check if that pagenumber is there in the buffer
    if (t_htable->page_number == PageId_in_a_DB)
    {
      //pass the page
      page = &bufPool[t_htable->frame_number];
      //increase the pincount
      frmeTable[t_htable->frame_number].pin();
      return OK;
    }
  }

  //The page is not found in the buffer..We need a new one
  int t_frame_chosen;
  Status state;
  if (MRU_hated_head != NULL)
  {
    struct MRU_hated *t_MRU = MRU_hated_head->next;
    t_frame_chosen = MRU_hated_head->frame_number;
    //check if the dirty bit is set for that frame than write it back...
    //free that frame from the MRU hated list;
    delete MRU_hated_head;
    MRU_hated_head = t_MRU; //update the head to the next element
  }
  else if (LRU_loved_head != NULL)
  {
    struct LRU_loved *t_LRU = LRU_loved_head->next;
    t_frame_chosen = LRU_loved_head->frame_number;
    //check if the dirty bit is set for that frame than write it back...
    //free that frame from the LRU loved list;
    delete LRU_loved_head;
    LRU_loved_head = t_LRU; //update the head to the next element
  }
  else
  {
    cout << "No frame found for replacement" << endl;
  }
  if (frmeTable[t_frame_chosen].r_dirty_bit() == true)
  {
    state = MINIBASE_DB->write_page(frmeTable[t_frame_chosen].pageNo, &bufPool[t_frame_chosen]);
  }
  //Read the page from the database
  state = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[t_frame_chosen]);
  //initializa that frame to default
  frmeTable[t_frame_chosen].set_dirtybit(false);
  frmeTable[t_frame_chosen].setPageNo(PageId_in_a_DB);
  frmeTable[t_frame_chosen].pin();
  //check the pincout
  if (frmeTable[t_frame_chosen].pin_count() == 0)
  {
    cout << "check the pincount there is problem in newPage function" << endl;
  }

  page = &bufPool[t_frame_chosen];

  //see the replacement frame already exist in hash table if yes than
  for (int i = 0; i < HTSIZE; i++)
  {
    t_htable = &hashTable[i];
    prev_htable = NULL;
    for (; t_htable != NULL; t_htable = t_htable->next)
    {
      //find empty one and replace it
      if (t_htable->frame_number == t_frame_chosen)
      {

        t_htable->page_number = INVALID_PAGE;
        t_htable->frame_number = INVALID_PAGE;
        if (t_htable->next != NULL && t_htable == &hashTable[i])
        {
          //copy the content of the t_htable->next to t_htable and delete that page
          t_htable->page_number = t_htable->next->page_number;
          t_htable->frame_number = t_htable->next->frame_number;
          struct hash_table *tmp_table = t_htable->next;
          t_htable->next = t_htable->next->next;
          //now free  tmp_table
          delete tmp_table;
        }
        else
        {
          prev_htable->next = t_htable->next;
          delete t_htable;
        }
        goto add_frame_to_hash;
      }
      prev_htable = t_htable;
    }
  }
add_frame_to_hash:

  t_htable = &hashTable[hvalue];

  for (; t_htable != NULL; t_htable = t_htable->next)
  {
    //find empty one and replace it
    if (t_htable->page_number == -1 && t_htable == &hashTable[hvalue])
    {
      t_htable->page_number = PageId_in_a_DB;
      t_htable->frame_number = t_frame_chosen;
      t_htable->next = NULL;
      return OK;
    }
    else if (t_htable->next == NULL)
    {
      t_htable->next = new hash_table;
      t_htable = t_htable->next;
      t_htable->page_number = PageId_in_a_DB;
      t_htable->frame_number = t_frame_chosen;
      t_htable->next = NULL;
      return OK;
    }
  }
  return OK;
} //end pinPage

//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
// will have to do HATE AND LOVE FOR EVERY ITERATION
Status BufMgr::unpinPage(PageId page_num, int dirty = FALSE, int hate = FALSE)
{ // put your code here
  //find the corresponding frame
    Page *t_page;
      t_page=new Page;
  if (MINIBASE_DB->read_page(page_num, t_page) != OK )
  {
    return FAIL;
  }
    delete t_page;
  int hvalue = (page_num) % HTSIZE;
  hash_table *t_htable = &hashTable[hvalue];
  hash_table *prev_htable;
  bool found_frame = false;
  
  for (; t_htable != NULL; t_htable = t_htable->next)
  {
    //check if that pagenumber is there in the buffer
    if (t_htable->page_number == page_num)
    {
      found_frame = true;
      break;
    }
    prev_htable = t_htable;
  }

  if (found_frame == true)
  {
    if (frmeTable[t_htable->frame_number].pin_count() == 0)
    {
      cout << bufErrMsgs[11] << endl;
      return BUFMGR;
    }
    int t_frameNum = t_htable->frame_number;
    //set the dirty bits
    frmeTable[t_htable->frame_number].set_dirtybit(dirty);
    //decrease the pincount
    if (frmeTable[t_htable->frame_number].pin_count() > 0)
    {
      frmeTable[t_htable->frame_number].unpin();
    }

    if (frmeTable[t_htable->frame_number].pin_count() != 0)
    {
      return OK;
    }

    //check if this exist in the struct of MRU_hated and delete it

    if (hate == true)
    {
      struct MRU_hated *t_MRU = MRU_hated_head;
      struct MRU_hated *prev_MRU = NULL;
      for (; t_MRU != NULL; t_MRU = t_MRU->next)
      {
        if (t_MRU->frame_number == t_frameNum)
        {
          if (prev_MRU == NULL)
          {
            delete t_MRU;
            MRU_hated_head = NULL;
            break;
          }
          else
          {
            prev_MRU->next = t_MRU->next;
            delete t_MRU;
            t_MRU = prev_MRU;
            break;
          }
        }
        prev_MRU = t_MRU;
      }

      if (prev_MRU != NULL && hate == true)
      {
        //enter the page at begining
        t_MRU = new MRU_hated;
        t_MRU->next = MRU_hated_head;
        t_MRU->frame_number = t_frameNum;
        MRU_hated_head = t_MRU;
      }
      else if (prev_MRU == NULL && hate == true)
      {
        MRU_hated_head = new MRU_hated; // malloc(sizeof(struct MRU_hated));
        MRU_hated_head->frame_number = t_frameNum;
        MRU_hated_head->next = NULL;
      }
    }
    else
    {
      struct LRU_loved *t_LRU = LRU_loved_head;
      struct LRU_loved *prev_LRU = NULL;
      for (; t_LRU != NULL; t_LRU = t_LRU->next)
      {
        if (t_LRU->frame_number == t_frameNum)
        {
          if (prev_LRU == NULL)
          {
            delete t_LRU;
            LRU_loved_head = NULL;
            break;
          }
          else
          {
            prev_LRU->next = t_LRU->next;
            delete t_LRU;
            t_LRU = prev_LRU;
          }
        }
        prev_LRU = t_LRU;
      }
      if (prev_LRU != NULL && hate == false)
      {
        prev_LRU->next = new LRU_loved; //malloc(sizeof(struct LRU_loved));
        prev_LRU->next->frame_number = t_frameNum;
        prev_LRU->next->next = NULL;
      }
      else if (prev_LRU == NULL && hate == false)
      {
        LRU_loved_head = new LRU_loved; //malloc(sizeof(struct LRU_loved));
        LRU_loved_head->frame_number = t_frameNum;
        LRU_loved_head->next = NULL;
      }
    }
  }
  else
  {
    //  cout << "replacement frame not found\n";
    return FAIL;
  }
  return OK;
}

//*************************************************************
//** This is the implementation of newPage
//************************************************************
Status BufMgr::newPage(PageId &firstPageId, Page *&firstpage, int howmany)
{
  // put your code here
  //check this page as per the MRU policy of the most hated page recently...that is the last page used
  //for loop depending on the size of the howmany
  Status state;

  int t_frame_chosen;

  if (LRU_loved_head != NULL || MRU_hated_head != NULL)
  {
    state = MINIBASE_DB->allocate_page(firstPageId, howmany);
    state = pinPage(firstPageId, firstpage, 0);
  }
  else
  {
    return FAIL;
    cout << "No frame found for replacement" << endl;
    //deallocate all the pages and return error
    for (unsigned int i = 0; i < numBuffers; i++)
      state = MINIBASE_DB->deallocate_page(frmeTable[i].getPageNo(), 1);
    return FAIL;
  }
  return OK;
}

//*************************************************************
//** This is the implementation of freePage
//************************************************************
Status BufMgr::freePage(PageId globalPageId)
{
  // put your code here

  //search the page in hash table
  int hvalue = (globalPageId) % HTSIZE;
  hash_table *t_htable = &hashTable[hvalue];
  hash_table *prev_htable;
  bool found_frame = false;
  for (; t_htable != NULL; t_htable = t_htable->next)
  {
    //check if that pagenumber is there in the buffer
    if (t_htable->page_number == globalPageId)
    {
      found_frame = true;
      break;
    }
    prev_htable = t_htable;
  }
  if (frmeTable[t_htable->frame_number].pin_count() != 0)
  {
    return FAIL;
  }

  Status state = MINIBASE_DB->deallocate_page(globalPageId, 1);
  return OK;
}

//*************************************************************
//** This is the implementation of flushPage
//************************************************************
Status BufMgr::flushPage(PageId pageid)
{
  // put your code here
 int hvalue = (pageid) % HTSIZE;
  hash_table *t_htable = &hashTable[hvalue];
  hash_table *prev_htable;
  bool found_frame = false;
  for (; t_htable != NULL; t_htable = t_htable->next)
  {
    //check if that pagenumber is there in the buffer
    if (t_htable->page_number == pageid)
    {
      found_frame = true;
      break;
    }
    prev_htable = t_htable;
  }
  Status state = MINIBASE_DB->write_page(frmeTable[t_htable->frame_number].pageNo, &bufPool[t_htable->frame_number]);
  return OK;
}

//*************************************************************
//** This is the implementation of flushAllPages
//************************************************************
Status BufMgr::flushAllPages()
{
  //put your code here
  for (int i = 0; i < numBuffers; i++)
  {
    if (frmeTable[i].pageNo != -1)
      Status state = MINIBASE_DB->write_page(frmeTable[i].pageNo, &bufPool[i]);
  }
  return OK;
}

/*** Methods for compatibility with project 1 ***/
//*************************************************************
//** This is the implementation of pinPage
//************************************************************
Status BufMgr::pinPage(PageId PageId_in_a_DB, Page *&page, int emptyPage, const char *filename)
{
    Page *t_page;
      t_page=new Page;

  if (MINIBASE_DB->read_page(PageId_in_a_DB, t_page) != OK )
  {
    return FAIL;
  }
  delete t_page;
  int hvalue = (PageId_in_a_DB) % HTSIZE;
  hash_table *t_htable = &hashTable[hvalue];
  hash_table *prev_htable;

  for (; t_htable != NULL; t_htable = t_htable->next)
  {
    //check if that pagenumber is there in the buffer
    if (t_htable->page_number == PageId_in_a_DB)
    {
      //pass the page
      page = &bufPool[t_htable->frame_number];
      //increase the pincount
      frmeTable[t_htable->frame_number].pin();
      return OK;
    }
  }

  //The page is not found in the buffer..We need a new one
  int t_frame_chosen;
  Status state;
  if (MRU_hated_head != NULL)
  {
    struct MRU_hated *t_MRU = MRU_hated_head->next;
    t_frame_chosen = MRU_hated_head->frame_number;
    //check if the dirty bit is set for that frame than write it back...
    //free that frame from the MRU hated list;
    delete MRU_hated_head;
    MRU_hated_head = t_MRU; //update the head to the next element
  }
  else if (LRU_loved_head != NULL)
  {
    struct LRU_loved *t_LRU = LRU_loved_head->next;
    t_frame_chosen = LRU_loved_head->frame_number;
    //check if the dirty bit is set for that frame than write it back...
    //free that frame from the LRU loved list;
    delete LRU_loved_head;
    LRU_loved_head = t_LRU; //update the head to the next element
  }
  else
  {
    cout << "No frame found for replacement" << endl;
  }
  //  cout << "Frame chosen for replacement " << t_frame_chosen << endl;
  if (frmeTable[t_frame_chosen].r_dirty_bit() == true)
  {
    state = MINIBASE_DB->write_page(frmeTable[t_frame_chosen].pageNo, &bufPool[t_frame_chosen]);
    //write it back to the db disk
  }
  //Read the page from the database
  state = MINIBASE_DB->read_page(PageId_in_a_DB, &bufPool[t_frame_chosen]);
  //initializa that frame to default
  frmeTable[t_frame_chosen].set_dirtybit(false);
  frmeTable[t_frame_chosen].setPageNo(PageId_in_a_DB);
  frmeTable[t_frame_chosen].pin();
  //check the pincout
  if (frmeTable[t_frame_chosen].pin_count() == 0)
  {
    cout << "check the pincount there is problem in newPage function" << endl;
  }

  page = &bufPool[t_frame_chosen];

  //see the replacement frame already exist in hash table if yes than
  for (int i = 0; i < HTSIZE; i++)
  {
    t_htable = &hashTable[i];
    prev_htable = NULL;
    for (; t_htable != NULL; t_htable = t_htable->next)
    {
      //find empty one and replace it
      if (t_htable->frame_number == t_frame_chosen)
      {

        t_htable->page_number = INVALID_PAGE;
        if (t_htable->next != NULL && t_htable == &hashTable[i])
        {
          //copy the content of the t_htable->next to t_htable and delete that page
          t_htable->page_number = t_htable->next->page_number;
          t_htable->frame_number = t_htable->next->frame_number;
          struct hash_table *tmp_table = t_htable->next;
          t_htable->next = t_htable->next->next;
          //now free  tmp_table
          delete tmp_table;
        }
        else
        {
          prev_htable->next = t_htable->next;
          delete t_htable;
        }
        goto add_frame_to_hash;
      }
      prev_htable = t_htable;
    }
  }
add_frame_to_hash:

  t_htable = &hashTable[hvalue];

  for (; t_htable != NULL; t_htable = t_htable->next)
  {
    //find empty one and replace it
    if (t_htable->page_number == -1 && t_htable == &hashTable[hvalue])
    {
      t_htable->page_number = PageId_in_a_DB;
      t_htable->frame_number = t_frame_chosen;
      t_htable->next = NULL;
      //   cout << "Successfullyu added to hasttabel" << endl;
      return OK;
    }
    else if (t_htable->next == NULL)
    {
      t_htable->next = new hash_table;
      t_htable = t_htable->next;
      t_htable->page_number = PageId_in_a_DB;
      t_htable->frame_number = t_frame_chosen;
      t_htable->next = NULL;
      //   cout << "Successfullyu added to hasttabel in overflow bucket" << endl;
      return OK;
    }
  }
  return OK;
}
//*************************************************************
//** This is the implementation of unpinPage
//************************************************************
Status BufMgr::unpinPage(PageId globalPageId_in_a_DB, int dirty, const char *filename)
{
  //put your code here
  Page *t_page;
    t_page=new Page;
  if (MINIBASE_DB->read_page(globalPageId_in_a_DB, t_page) != OK )
  {
    return FAIL;
  }
  delete t_page;
  //find the corresponding frame
  int hate = FALSE;
  int hvalue = (globalPageId_in_a_DB) % HTSIZE;
  hash_table *t_htable = &hashTable[hvalue];
  hash_table *prev_htable;
  bool found_frame = false;
  for (; t_htable != NULL; t_htable = t_htable->next)
  {
    //check if that pagenumber is there in the buffer
    if (t_htable->page_number == globalPageId_in_a_DB)
    {
      found_frame = true;
      break;
    }
    prev_htable = t_htable;
  }

  if (found_frame == true)
  {
    if (frmeTable[t_htable->frame_number].pin_count() == 0)
    {
      cout << bufErrMsgs[11] << endl;
      return BUFMGR;
    }
    int t_frameNum = t_htable->frame_number;
    //set the dirty bits
    frmeTable[t_htable->frame_number].set_dirtybit(dirty);
    //decrease the pincount
    if (frmeTable[t_htable->frame_number].pin_count() > 0)
    {
      frmeTable[t_htable->frame_number].unpin();
    }

    if (frmeTable[t_htable->frame_number].pin_count() != 0)
    {
      return OK;
    }

    //check if this exist in the struct of MRU_hated and delete it

    if (hate == true)
    {
      struct MRU_hated *t_MRU = MRU_hated_head;
      struct MRU_hated *prev_MRU = NULL;
      for (; t_MRU != NULL; t_MRU = t_MRU->next)
      {
        if (t_MRU->frame_number == t_frameNum)
        {
          if (prev_MRU == NULL)
          {
            delete t_MRU;
            MRU_hated_head = NULL;
            break;
          }
          else
          {
            prev_MRU->next = t_MRU->next;
            delete t_MRU;
            t_MRU = prev_MRU;
            break;
          }
        }
        prev_MRU = t_MRU;
      }

      if (prev_MRU != NULL && hate == true)
      {
        //enter the page at begining
        t_MRU = new MRU_hated;
        t_MRU->next = MRU_hated_head;
        t_MRU->frame_number = t_frameNum;
        MRU_hated_head = t_MRU;
      }
      else if (prev_MRU == NULL && hate == true)
      {
        MRU_hated_head = new MRU_hated; // malloc(sizeof(struct MRU_hated));
        MRU_hated_head->frame_number = t_frameNum;
        MRU_hated_head->next = NULL;
      }
    }
    else
    {
      struct LRU_loved *t_LRU = LRU_loved_head;
      struct LRU_loved *prev_LRU = NULL;
      for (; t_LRU != NULL; t_LRU = t_LRU->next)
      {
        if (t_LRU->frame_number == t_frameNum)
        {
          if (prev_LRU == NULL)
          {
            delete t_LRU;
            LRU_loved_head = NULL;
            break;
          }
          else
          {
            prev_LRU->next = t_LRU->next;
            delete t_LRU;
            t_LRU = prev_LRU;
          }
        }
        prev_LRU = t_LRU;
      }
      if (prev_LRU != NULL && hate == false)
      {
        prev_LRU->next = new LRU_loved; //malloc(sizeof(struct LRU_loved));
        prev_LRU->next->frame_number = t_frameNum;
        prev_LRU->next->next = NULL;
      }
      else if (prev_LRU == NULL && hate == false)
      {
        LRU_loved_head = new LRU_loved; //malloc(sizeof(struct LRU_loved));
        LRU_loved_head->frame_number = t_frameNum;
        LRU_loved_head->next = NULL;
      }
    }
  }
  else
  {
    //  cout << "replacement frame not found\n";
    return FAIL;
  }
  return OK;
}

//*************************************************************
//** This is the implementation of getNumUnpinnedBuffers
//************************************************************
unsigned int BufMgr::getNumUnpinnedBuffers()
{
  unsigned int count = 0;
  for (int i = 0; i < numBuffers; i++)
  {
    if (frmeTable[i].pin_count() == 0)
    {
      count++;
    }
  }
  return count;
}
