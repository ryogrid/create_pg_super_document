# SharedInvalidationMessage

## Location
src/include/storage/sinval.h: 122 - 153

## Overview
SharedInvalidationMessage is a union structure that encapsulates all types of shared invalidation messages in PostgreSQL, enabling efficient cache invalidation across multiple processes when database objects are modified.

## Definition


## Detailed Description
SharedInvalidationMessage serves as PostgreSQL's primary mechanism for maintaining cache consistency across multiple backend processes. This union structure allows different types of invalidation messages to be handled through a single interface while maintaining type safety through the discriminating id field.

The union design enables efficient message passing through shared memory segments, with each message type optimized for its specific invalidation purpose. All message types begin with an int8 id field that identifies the message type, allowing receivers to properly discriminate and process the appropriate invalidation action.

The system supports six distinct invalidation types:
- Catalog cache (catcache) invalidations for system catalog entries
- Catalog invalidations for entire catalog contents
- Relation cache (relcache) invalidations for relation metadata
- Storage manager (smgr) invalidations for file-level operations
- Relation map (relmap) invalidations for system catalog file mappings
- Snapshot invalidations for snapshot-related cache entries

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Discriminating type field present in all union members, indicates which specific message type is active
- : SharedInvalCatcacheMsg for catalog cache invalidation messages (id = SHAREDINVALCATCACHE_ID)
- : SharedInvalCatalogMsg for catalog invalidation messages (id = SHAREDINVALCATALOG_ID = -1)
- : SharedInvalRelcacheMsg for relation cache invalidation messages (id = SHAREDINVALRELCACHE_ID = -2)
- : SharedInvalSmgrMsg for storage manager invalidation messages (id = SHAREDINVALSMGR_ID = -3)
- : SharedInvalRelmapMsg for relation map invalidation messages (id = SHAREDINVALRELMAP_ID = -4)
- : SharedInvalSnapshotMsg for snapshot invalidation messages (id = SHAREDINVALSNAPSHOT_ID = -5)

## Dependencies
- Functions called/Symbols referenced:
  - int8 (PostgreSQL's 8-bit signed integer type)
  - SharedInvalCatcacheMsg, SharedInvalCatalogMsg, SharedInvalRelcacheMsg
  - SharedInvalSmgrMsg, SharedInvalRelmapMsg, SharedInvalSnapshotMsg
  - Various ID constants (SHAREDINVALCATCACHE_ID through SHAREDINVALSNAPSHOT_ID)

- Called from (representative examples):
  - SendSharedInvalidMessages (sends messages to other processes)
  - ReceiveSharedInvalidMessages (receives and processes messages)
  - LocalExecuteInvalidationMessage (processes individual messages)
  - xactGetCommittedInvalidationMessages (retrieves transaction invalidations)
  - ProcessCommittedInvalidationMessages (processes committed transaction invalidations)
  - AddInvalidationMessage (adds messages to pending list)

## Notes and Other Information
- The union design ensures all message types have the same memory footprint, optimizing shared memory usage
- The id field must always be the first member in each struct to enable proper union discrimination
- Messages are processed through PostgreSQL's shared invalidation infrastructure using circular buffers in shared memory
- Each message type corresponds to a specific cache or storage subsystem that needs invalidation
- The system handles both immediate processing and deferred processing of invalidation messages
- Critical for maintaining ACID properties and consistency in a multi-process PostgreSQL environment
- Used extensively in transaction commit processing, DDL operations, and logical replication scenarios