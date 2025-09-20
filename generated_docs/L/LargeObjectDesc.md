# LargeObjectDesc

## Location
[src/include/storage/large_object.h:39-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/large_object.h#L39-L51)

## Overview
A structure that represents the state of a currently-open large object in PostgreSQL, containing metadata necessary for read/write operations on large objects.

## Definition

```c
typedef struct LargeObjectDesc
{
	Oid			id;				/* LO's identifier */
	Snapshot	snapshot;		/* snapshot to use */
	SubTransactionId subid;		/* owning subtransaction ID */
	uint64		offset;			/* current seek pointer */
	int			flags;			/* see flag bits below */

/* bits in flags: */
#define IFS_RDLOCK		(1 << 0)	/* LO was opened for reading */
#define IFS_WRLOCK		(1 << 1)	/* LO was opened for writing */

} LargeObjectDesc;
```
## Detailed Description
The LargeObjectDesc structure serves as a descriptor for managing large objects that are currently open in a PostgreSQL session. It maintains essential state information including the object's logical identifier, snapshot for transaction isolation, ownership tracking, current position within the object, and access permissions.

As of PostgreSQL version 11, permission checks are performed when the large object is opened, with the IFS_RDLOCK and IFS_WRLOCK flags indicating that read or write access has been both requested and verified. Prior to version 7.1, this structure also needed to track references to separate tables and indexes for each large object, but all large objects now reside in the pg_largeobject system catalog.

## Parameters / Member Variables
- `id`: The logical OID (Object Identifier) that uniquely identifies the large object within the database
- `snapshot`: The snapshot context used for read/write operations to ensure proper transaction isolation and MVCC behavior
- `subid`: The subtransaction ID that currently owns this descriptor, used for proper cleanup during subtransaction rollbacks
- `offset`: Current seek position within the large object, allowing for random access operations
- `flags`: Bitwise flags indicating the access mode and permissions:
  - `IFS_RDLOCK`: Set when the large object was opened for reading and read permission has been verified
  - `IFS_WRLOCK`: Set when the large object was opened for writing and write permission has been verified

## Dependencies
- Functions called/Symbols referenced:
  - Oid
  - [Snapshot](../S/Snapshot.md)
  - SubTransactionId
- Called from (representative examples):
  - [be_lo_open](../b/be_lo_open.md)
  - [inv_open](../i/inv_open.md)
  - [inv_close](../i/inv_close.md)
  - [inv_read](../i/inv_read.md)
  - [inv_write](../i/inv_write.md)
  - [inv_seek](../i/inv_seek.md)
  - [inv_tell](../i/inv_tell.md)
  - [inv_getsize](../i/inv_getsize.md)
  - [inv_truncate](../i/inv_truncate.md)
  - [lo_read](../l/lo_read.md)
  - [lo_write](../l/lo_write.md)
  - [newLOfd](../n/newLOfd.md)
  - [closeLOfd](../c/closeLOfd.md)

## Notes and Other Information
- Permission checks are now performed at open time rather than on each operation, improving performance for repeated access
- The structure is used extensively throughout the large object API in both the backend storage layer (inv_api.c) and the frontend stub functions (be-fsstubs.c)
- All large objects are now stored in the unified pg_largeobject system catalog, simplifying the implementation compared to earlier PostgreSQL versions
- The descriptor must be properly managed across subtransaction boundaries to ensure correct cleanup and rollback behavior