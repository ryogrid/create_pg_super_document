# DropDatabaseBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:4376-4413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4376-L4413)

## Overview
Removes all buffers in the buffer cache for a particular database, dropping dirty pages without writing them to disk when a database is being destroyed.

## Definition

```c
void
DropDatabaseBuffers(Oid dbid)
```
## Detailed Description
This function removes all buffers belonging to a specific database from the PostgreSQL shared buffer pool. It is primarily used during database destruction operations where the database directory tree no longer exists, making it unnecessary (and impossible) to flush dirty pages to disk. The function iterates through all buffers in the shared buffer pool and invalidates any buffer that belongs to the target database.

The implementation performs an unlocked precheck on each buffer's database OID for performance, only acquiring the buffer header lock when a match is found. This approach is safe because the database being dropped cannot be the current database (local buffers are not considered), ensuring no concurrent access conflicts.

## Parameters / Member Variables
- : The OID (Object Identifier) of the database whose buffers should be dropped from the buffer cache

## Dependencies
- Functions called/Symbols referenced:
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [LockBufHdr](../L/LockBufHdr.md)  
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
- Types used:
  - [BufferDesc](../B/BufferDesc.md)
- Called from (representative examples):
  - [createdb_failure_callback](../c/createdb_failure_callback.md)
  - [dropdb](../d/dropdb.md)
  - [movedb](../m/movedb.md)
  - [dbase_redo](../d/dbase_redo.md)

## Notes and Other Information
- This function does not consider local buffers since the target database cannot be the current database by assumption
- Dirty pages are simply dropped without being written to disk, which is safe only during database destruction
- Uses an unlocked precheck optimization to avoid unnecessary locking when buffer database OIDs don't match
- Implementation is similar to DropRelationBuffers() but operates at the database level rather than relation level
- The function ensures proper locking protocol by re-checking the database OID after acquiring the buffer header lock

## Simplified Source

```c
void
DropDatabaseBuffers(Oid dbid)
{
    int i;

    // Iterate through all buffers in the shared buffer pool
    // (Local buffers not considered - target DB can't be our own)
    for (i = 0; i < NBuffers; i++) {
        BufferDesc *bufHdr = GetBufferDescriptor(i);
        uint32 buf_state;

        // Quick unlocked check for performance
        if (bufHdr->tag.dbOid != dbid)
            continue;

        // Lock buffer and double-check database OID
        buf_state = LockBufHdr(bufHdr);
        if (bufHdr->tag.dbOid == dbid)
            InvalidateBuffer(bufHdr);  // Releases spinlock
        else
            UnlockBufHdr(bufHdr, buf_state);
    }
}
```