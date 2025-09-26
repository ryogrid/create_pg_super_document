# SharedInvalSnapshotMsg

## Location
[src/include/storage/sinval.h:111-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/sinval.h#L111-L121)

## Overview
SharedInvalSnapshotMsg is a structure that represents a shared invalidation message for snapshots, used to invalidate snapshot-related cache entries across PostgreSQL processes when certain database objects are modified.

## Definition

```c
typedef union
{
	int8		id;				/* type field --- must be first */
	SharedInvalCatcacheMsg cc;
	SharedInvalCatalogMsg cat;
	SharedInvalRelcacheMsg rc;
	SharedInvalSmgrMsg sm;
	SharedInvalRelmapMsg rm;
	SharedInvalSnapshotMsg sn;
} SharedInvalidationMessage;
```
## Detailed Description
SharedInvalSnapshotMsg is part of PostgreSQL's shared invalidation messaging system that ensures cache consistency across multiple processes. This specific message type handles invalidation of snapshot-related cache entries when relations are modified. The structure is designed to identify which database and relation require snapshot cache invalidation, allowing the system to maintain consistent snapshot behavior across concurrent transactions.

The message uses a union-based design where it's part of the larger SharedInvalidationMessage union, allowing efficient message passing through shared memory. The id field is set to SHAREDINVALSNAPSHOT_ID (-5) to identify this message type among other invalidation message types.

## Parameters / Member Variables
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Type identifier field that must be first in the structure, set to SHAREDINVALSNAPSHOT_ID (-5) to identify this as a snapshot invalidation message
- : Database OID identifying which database the invalidation applies to, or 0 if it applies to shared relations across all databases
- : Relation OID identifying the specific relation that requires snapshot cache invalidation

## Dependencies
- Functions called/Symbols referenced:
  - int8 (PostgreSQL's 8-bit signed integer type)
  - Oid (PostgreSQL's object identifier type)
  - SHAREDINVALSNAPSHOT_ID (constant value -5)

- Called from (representative examples):
  - AddSnapshotInvalidationMessage (creates and populates these messages)
  - LocalExecuteInvalidationMessage (processes these messages)
  - standby_desc_invalidations (describes these messages for WAL logging)

## Notes and Other Information
- This structure is part of the SharedInvalidationMessage union and follows the same memory layout requirements
- The id field must always be first to enable proper union discrimination
- Used specifically for snapshot cache invalidation, distinct from other cache invalidation types like catcache or relcache
- The message is processed through PostgreSQL's shared invalidation infrastructure to ensure all processes receive and act on the invalidation
- Snapshot invalidations are particularly important for maintaining MVCC (Multi-Version Concurrency Control) consistency across transactions