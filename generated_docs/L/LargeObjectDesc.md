# LargeObjectDesc

## Location
src/include/storage/large_object.h: 39 - 51

## Overview
A structure that represents the state of a currently-open large object in PostgreSQL, containing metadata necessary for read/write operations on large objects.

## Definition


## Detailed Description
The LargeObjectDesc structure serves as a descriptor for managing large objects that are currently open in a PostgreSQL session. It maintains essential state information including the object's logical identifier, snapshot for transaction isolation, ownership tracking, current position within the object, and access permissions.

As of PostgreSQL version 11, permission checks are performed when the large object is opened, with the IFS_RDLOCK and IFS_WRLOCK flags indicating that read or write access has been both requested and verified. Prior to version 7.1, this structure also needed to track references to separate tables and indexes for each large object, but all large objects now reside in the pg_largeobject system catalog.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): The logical OID (Object Identifier) that uniquely identifies the large object within the database
- : The snapshot context used for read/write operations to ensure proper transaction isolation and MVCC behavior
- : The subtransaction ID that currently owns this descriptor, used for proper cleanup during subtransaction rollbacks
- : Current seek position within the large object, allowing for random access operations
- : Bitwise flags indicating the access mode and permissions:
  - : Set when the large object was opened for reading and read permission has been verified
  - : Set when the large object was opened for writing and write permission has been verified

## Dependencies
- Functions called/Symbols referenced:
  - Oid
  - Snapshot
  - SubTransactionId
- Called from (representative examples):
  - be_lo_open
  - inv_open
  - inv_close
  - inv_read
  - inv_write
  - inv_seek
  - inv_tell
  - inv_getsize
  - inv_truncate
  - lo_read
  - lo_write
  - newLOfd
  - closeLOfd

## Notes and Other Information
- Permission checks are now performed at open time rather than on each operation, improving performance for repeated access
- The structure is used extensively throughout the large object API in both the backend storage layer (inv_api.c) and the frontend stub functions (be-fsstubs.c)
- All large objects are now stored in the unified pg_largeobject system catalog, simplifying the implementation compared to earlier PostgreSQL versions
- The descriptor must be properly managed across subtransaction boundaries to ensure correct cleanup and rollback behavior