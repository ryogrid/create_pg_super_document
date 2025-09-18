# DropDatabaseBuffers

## Location
src/backend/storage/buffer/bufmgr.c: 4376 - 4413

## Overview
Removes all buffers in the buffer cache for a particular database, dropping dirty pages without writing them to disk when a database is being destroyed.

## Definition


## Detailed Description
This function removes all buffers belonging to a specific database from the PostgreSQL shared buffer pool. It is primarily used during database destruction operations where the database directory tree no longer exists, making it unnecessary (and impossible) to flush dirty pages to disk. The function iterates through all buffers in the shared buffer pool and invalidates any buffer that belongs to the target database.

The implementation performs an unlocked precheck on each buffer's database OID for performance, only acquiring the buffer header lock when a match is found. This approach is safe because the database being dropped cannot be the current database (local buffers are not considered), ensuring no concurrent access conflicts.

## Parameters / Member Variables
- : The OID (Object Identifier) of the database whose buffers should be dropped from the buffer cache

## Dependencies
- Functions called/Symbols referenced:
  - GetBufferDescriptor
  - LockBufHdr  
  - InvalidateBuffer
  - UnlockBufHdr
- Types used:
  - BufferDesc
- Called from (representative examples):
  - createdb_failure_callback
  - dropdb
  - movedb
  - dbase_redo

## Notes and Other Information
- This function does not consider local buffers since the target database cannot be the current database by assumption
- Dirty pages are simply dropped without being written to disk, which is safe only during database destruction
- Uses an unlocked precheck optimization to avoid unnecessary locking when buffer database OIDs don't match
- Implementation is similar to DropRelationBuffers() but operates at the database level rather than relation level
- The function ensures proper locking protocol by re-checking the database OID after acquiring the buffer header lock