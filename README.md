Class projects for Class Database Implementation

Refer to the minibase components here:

http://research.cs.wisc.edu/coral/minibase/project.html

In makefile use -no-pie when compiling in Ubuntu OS, otherwise remove it

We implemented 

1. hfpage.C in Proj1 (This is how records are stored in page)

2. Heapfile.C and scan.C are implemented in Proj2 (This is how heapfile is seen as a collection of records . Internally records are stored on collection of HFPage onjects)

3. buf.C in Proj3 (This is how Buffer manager is managed)

4. sortedpage.C, btindexpage.C, btleafpage.C, key.C and btfile.C are implmented in proj4 (To make B+ tree to manage records)

5. sortmerge.C in proj4 (To sort records)




