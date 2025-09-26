# SubXactInfo

## Location
[src/backend/replication/logical/worker.c:341-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L341-L346)

## Overview
SubXactInfo is a structure that tracks the location of subtransaction information stored in temporary files during logical replication processing.

## Definition

```c
typedef struct SubXactInfo
{
	TransactionId xid;			/* XID of the subxact */
	int			fileno;			/* file number in the buffile */
	off_t		offset;			/* offset in the file */
} SubXactInfo;
```
## Detailed Description
SubXactInfo serves as a metadata record for subtransactions in logical replication. When processing large transactions that contain multiple subtransactions, the logical replication worker may need to spill subtransaction data to temporary files to manage memory usage. This structure maintains the mapping between a subtransaction ID and its storage location within the buffer file system, enabling efficient retrieval of subtransaction data when needed during the apply process.

## Parameters / Member Variables
- : TransactionId of the subtransaction being tracked
- : File number within the buffer file set where the subtransaction data is stored
- : Byte offset within the specified file where the subtransaction data begins

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId
  - off_t (standard POSIX type)
- Called from (representative examples):
  - [ApplySubXactData](../A/ApplySubXactData.md)
  - [subxact_info_write](../s/subxact_info_write.md)
  - [subxact_info_read](../s/subxact_info_read.md)
  - [subxact_info_add](../s/subxact_info_add.md)

## Notes and Other Information
This structure is part of the logical replication worker's memory management strategy for handling large transactions with multiple subtransactions. It works in conjunction with PostgreSQL's buffer file system to efficiently manage temporary storage of subtransaction data. The structure is primarily used in subtransaction management functions that handle reading, writing, and adding subtransaction information to the temporary file storage system.