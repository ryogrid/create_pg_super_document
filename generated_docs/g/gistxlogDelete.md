# gistxlogDelete

## Location
src/include/access/gistxlog.h: 50 - 59

## Overview
The  structure represents a WAL (Write-Ahead Logging) record for GiST index tuple deletion operations, capturing information needed to replay deletion operations during recovery.

## Definition


## Detailed Description
This structure is used to log GiST index tuple deletion operations in the write-ahead log. It contains all the information necessary to replay the deletion during crash recovery or streaming replication. The structure includes a snapshot conflict horizon for handling recovery conflicts, the number of tuples to delete, a flag for catalog relations, and a flexible array of offset numbers identifying which tuples to delete from the target page.

## Parameters / Member Variables
- : Transaction ID used to determine snapshot conflicts during recovery, ensuring proper MVCC visibility semantics
- : Number of index tuples being deleted in this operation
- : Boolean flag indicating if this is a catalog relation, used for handling recovery conflicts during logical decoding on standby servers
- : Flexible array containing the offset numbers of tuples to be deleted from the leaf page

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - gistRedoDeleteRecord
  - gistXLogDelete
  - out_gistxlogDelete
  - gist_desc
  - SizeOfGistxlogDelete

## Notes and Other Information
- This structure is used specifically for leaf pages in GiST indexes where index tuples are deleted
- The backup block 0 contains the leaf page whose index tuples are being deleted
- The flexible array member allows for variable-length records depending on the number of tuples being deleted
- Recovery conflict handling is particularly important for logical decoding scenarios on standby servers