/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>
#include <bits/stdc++.h>
#include "bt.h"

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
int keyCompare(const void *key1, const void *key2, AttrType t)
{
  // put your code here
  if (t == attrString)
  {
/*
    //char p1[200] = {""};
    //char p2[200] = {""};
    string p1((char *)key1);
    string p2((char *)key2);
    transform(p1.begin(), p1.end(), p1.begin(), ::tolower);
    transform(p2.begin(), p2.end(), p2.begin(), ::tolower);
    if (p1.compare(p2) == 0)
      return 0;
    if (p1.compare(p2) > 0)
      return 1;
    if (p1.compare(p2) < 0)
      return -1;

        */
        const char *p1 = static_cast<const char *>(key1);
     const char *p2 = static_cast<const char *>(key2);
    if (strcmp(p1, p2) == 0)
      return 0;
    else if (strcmp(p1, p2) > 0)
      return 1;
    else if (strcmp(p1, p2) < 0)
      return -1;
  
  }
  else if (t == attrInteger)
  {
    int p1 = 0;
    int p2 = 0;
    memcpy(&p1, key1, sizeof(int));
    memcpy(&p2, key2, sizeof(int));
    // const int *p2 = static_cast<const int *>(key2);
    if (p1 == p2)
      return 0;
    else if (p1 < p2)
      return -1;
    else if (p1 > p2)
      return 1;
  }
  else
  {
    cout << "Error" << endl;
    return -9999;
  }

  return 0;
}

/*
 * make_entry: write a <key,data> pair to a blob of   (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
void make_entry(KeyDataEntry *target,
                AttrType key_type, const void *key,
                nodetype ndtype, Datatype data,
                int *pentry_len)
{
  // put your code here

  if (key_type == attrString)
  {
    //copy the keyp
    // cout << "string name is " << (char *)key << " string length" << strlen((char *)key) << endl;
    int key_size = strlen((char *)key);
    memcpy(target, (char *)key, strlen((char *)key));
    char end = '\0';

    memcpy(((char *)target + key_size), &end, strlen((char *)key));
    key_size++;
    if (ndtype == INDEX)
    {
      //page No
      memcpy(((char *)target + key_size), &(data.pageNo), sizeof(int));
      *pentry_len = sizeof(int) + key_size;
    }
    else if (ndtype == LEAF)
    {
      memcpy(((char *)target + key_size), &(data.rid), 2 * sizeof(int));
      *pentry_len = 2 * sizeof(int) + key_size;
    }
  }
  if (key_type == attrInteger)
  {
    //  target->key.intkey = *(int *)key;
    memcpy(target, (int *)key, sizeof(int));
    if (ndtype == INDEX)
    {
      //page No
      memcpy(((char *)target + 4), &(data.pageNo), sizeof(int));
      *pentry_len = 2 * sizeof(int);
    }
    else if (ndtype == LEAF)
    {
      memcpy(((char *)target + 4), &(data.rid), 2 * sizeof(int));
      *pentry_len = 3 * sizeof(int);
    }
  }

  return;
}

/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a)  memory chunk holding the pair (*psource) and,b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetkey, Datatype *targetdata,
                  KeyDataEntry *psource, int entry_len, nodetype ndtype)
{
  // put your code here

  if (ndtype == INDEX)
  {
    int find_key_type = entry_len - sizeof(int);
    //this indicates its a integer key
    if (find_key_type == sizeof(int))
    {
      //copy the key
      memcpy(targetkey, (void *)psource, sizeof(int));
      //page No
      memcpy(targetdata, (int *)((int *)psource + 1), sizeof(int));
    }
    else
    {
      //copy the char key
      memcpy(targetkey, (char *)psource, find_key_type);
      memcpy(targetdata, ((char *)psource + find_key_type), sizeof(int) * 1);
    }
  }
  else if (ndtype == LEAF)
  {
    int find_key_type = entry_len - sizeof(RID);
    if (find_key_type == sizeof(int))
    {
      //copy the key
      memcpy(targetkey, (void *)psource, sizeof(int));
      //copy the rid
      memcpy(targetdata, ((int *)psource + 1), sizeof(int) * 2);
    }
    else
    {
      //copy the char key
      memcpy(targetkey, (char *)psource, find_key_type);
      //copy the rid
      memcpy(targetdata, ((char *)psource + find_key_type), sizeof(int) * 2);
    }
  }

  return;
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type)
{
  // put your code here
  if (key_type == attrString)
  {
    //condition for leaf length
    //condition for index length
    return strlen((char *)key);
  }
  else if (key_type == attrInteger)
  {
    //condition for leaf length
    //condition for index length
    return sizeof(int);
  }
  else
  {
    cout << "Error" << endl;
    return -9999;
  }
}

/*
 * get_key_data_length: return (key+data) length in given key_type
 */
int get_key_data_length(const void *key, const AttrType key_type,
                        const nodetype ndtype)
{
  // put your code here
  if (key_type == attrString)
  {
    //condition for leaf length
    //condition for index length
    //Page
    if (ndtype == INDEX)
      return strlen((char *)key) + sizeof(int);
    //RID
    if (ndtype == LEAF)
      return strlen((char *)key) + sizeof(int) * 2;
  }
  else if (key_type == attrInteger)
  {
    //condition for leaf length
    //condition for index length
    //Page
    if (ndtype == INDEX)
      return 2 * sizeof(int);
    //RID
    if (ndtype == LEAF)
      return 3 * sizeof(int);
  }
  else
  {
    cout << "Error" << endl;
    return -9999;
  }
}
