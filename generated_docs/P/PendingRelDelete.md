# PendingRelDelete

## Location
[src/backend/catalog/storage.c:61-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L61-L68)

## Overview
PendingRelDelete is a structure that represents relations scheduled for deletion or preservation in PostgreSQL's transaction management system, maintaining a linked list of relations that need to be handled at transaction commit or abort.

## Definition

```c
typedef struct PendingRelDelete
{
	RelFileLocator rlocator;	/* relation that may need to be deleted */
	ProcNumber	procNumber;		/* INVALID_PROC_NUMBER if not a temp rel */
	bool		atCommit;		/* T=delete at commit; F=delete at abort */
	int			nestLevel;		/* xact nesting level of request */
	struct PendingRelDelete *next;	/* linked-list link */
} PendingRelDelete;
```
## Detailed Description
PendingRelDelete is a crucial data structure in PostgreSQL's storage management system that tracks relations (tables, indexes, etc.) that require deferred deletion or preservation actions during transaction processing. The structure is part of a linked list system that allows PostgreSQL to postpone physical file operations until transaction boundaries are reached.

When a relation is created, PostgreSQL immediately creates the physical file but remembers it in this structure so it can be deleted if the transaction aborts. Conversely, when a relation is deleted, the deletion request is not executed immediately but is recorded in this list for execution at transaction commit.

The structure supports subtransaction handling through nesting levels, allowing proper cleanup and rollback behavior in complex transaction scenarios. All entries are maintained in TopMemoryContext to ensure they persist throughout the transaction lifecycle.

## Parameters / Member Variables
- : RelFileLocator identifying the specific relation that may need deletion or preservation
- : Process number for temporary relations, set to INVALID_PROC_NUMBER for non-temporary relations
- : Boolean flag determining the action timing - true means delete at commit, false means delete at abort
- : Transaction nesting level indicating the subtransaction depth where this request was made
- : Pointer to the next PendingRelDelete structure in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - ProcNumber
  - [RelFileLocator](../R/RelFileLocator.md) (implied from rlocator member)

- Called from (representative examples):
  - [RelationCreateStorage](../R/RelationCreateStorage.md)
  - [RelationDropStorage](../R/RelationDropStorage.md)  
  - [RelationPreserveStorage](../R/RelationPreserveStorage.md)
  - [smgrDoPendingDeletes](../s/smgrDoPendingDeletes.md)
  - [smgrDoPendingSyncs](../s/smgrDoPendingSyncs.md)
  - [smgrGetPendingDeletes](../s/smgrGetPendingDeletes.md)
  - [PostPrepare_smgr](PostPrepare_smgr.md)
  - [AtSubCommit_smgr](../A/AtSubCommit_smgr.md)
  - [SerializePendingSyncs](../S/SerializePendingSyncs.md)

## Notes and Other Information
- The linked list is maintained in TopMemoryContext to prevent premature deallocation
- Supports both regular and temporary relation handling through the procNumber field
- Critical for ACID properties by ensuring proper cleanup of filesystem resources
- Integrates with PostgreSQL's two-phase commit protocol for distributed transactions
- Part of the storage manager (smgr) subsystem responsible for low-level file operations
- Handles both creation rollback (delete files of aborted transactions) and deletion deferral (wait until commit to actually delete files)