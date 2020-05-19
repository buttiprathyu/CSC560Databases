/*
 * btreefilescan.h
 *
 * sample header file
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 *
 */

#ifndef _BTREEFILESCAN_H
#define _BTREEFILESCAN_H

#include "btfile.h"

// errors from this class should be defined in btfile.h

class BTreeFileScan : public IndexFileScan
{
  public:
    friend class BTreeFile;

    // get the next record
    Status get_next(RID &rid, void *keyptr);

    // delete the record currently scanned
    Status delete_current();

    int keysize(); // size of the key

    // destructor
    ~BTreeFileScan();
    const void *lo_key;
    const void *hi_key;
    PageId low_key_Id;
    PageId high_key_Id;
    RID first_datarid;
    RID last_datarid;
    RID current_rid; 
    AttrType Attr_type;
     char fname[200];
  private:
};

#endif
