# AtEOSubXact_cleanup

## Location
[src/backend/utils/cache/relcache.c:3444-3525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L3444-L3525)

## Overview
Handles cleanup of a single relation at subtransaction commit or abort, managing subtransaction ID transitions and determining whether relcache entries should be transferred to parent subtransactions or removed entirely.

## Definition

```c
enumber-in-subtransaction record
	 * or drop record.
	 */
	if (relation->rd_newRelfilelocatorSubid == mySubid)
	{
		if (isCommit)
			relation->rd_newRelfilelocatorSubid = parentSubid;
		else
			relation->rd_newRelfilelocatorSubid = InvalidSubTransactionId;
	}

	if (relation->rd_firstRelfilelocatorSubid == mySubid)
	{
		if (isCommit)
			relation->rd_firstRelfilelocatorSubid = parentSubid;
		else
			relation->rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
	}

	if (relation->rd_droppedSubid == mySubid)
	{
		if (isCommit)
			relation->rd_droppedSubid = parentSubid;
		else
			relation->rd_droppedSubid = InvalidSubTransactionId;
	}
}


/*
 *		RelationBuildLocalRelation
 *			Build a relcache entry for an about-to-be-created relation,
 *			and enter it into the relcache.
 */
Relation
RelationBuildLocalRelation(const char *relname,
						   Oid relnamespace,
						   TupleDesc tupDesc,
						   Oid relid,
						   Oid accessmtd,
						   RelFileNumber relfilenumber,
						   Oid reltablespace,
						   bool shared_relation,
						   bool mapped_relation,
						   char relpersistence,
						   char relkind)
{
	Relation	rel;
```
## Detailed Description
This static function performs subtransaction-specific cleanup for individual relation cache entries. It handles the complex logic of subtransaction state transitions by managing various subtransaction IDs associated with relations:

1. **Creation Subtransaction Handling**: For relations created in the current subtransaction (rd_createSubid == mySubid):
   - On commit: Transfers ownership to the parent subtransaction (unless the relation was also dropped)
   - On abort: Clears the relcache entry if possible, or transfers to parent if there are leaked references

2. **Relfilelocator Subtransaction Management**: Updates subtransaction IDs for relfilelocator changes:
   - rd_newRelfilelocatorSubid: Tracks when a relation gets a new relfilelocator
   - rd_firstRelfilelocatorSubid: Tracks the first relfilelocator change in a transaction hierarchy

3. **Drop Subtransaction Handling**: Manages relations marked as dropped in the current subtransaction (rd_droppedSubid == mySubid).

4. **Reference Count Safety**: Before removing relcache entries, checks for zero reference counts to prevent dangling pointer issues. If references exist, logs a warning and transfers ownership to the parent.

The function ensures proper cleanup while maintaining referential integrity and handling edge cases like leaked references gracefully.

## Parameters / Member Variables
- : The Relation object to be cleaned up
- : Boolean indicating whether this is a subtransaction commit (true) or abort (false)
- : The SubTransactionId of the subtransaction being terminated
- : The SubTransactionId of the parent subtransaction that will inherit any transferred state

## Dependencies
- Functions called/Symbols referenced:
  - RelationHasReferenceCountZero
  - [RelationClearRelation](../R/RelationClearRelation.md)
  - RelationGetRelationName
  - elog
- Constants used:
  - InvalidSubTransactionId
- [Relation](../R/Relation.md) fields modified:
  - rd_createSubid
  - rd_newRelfilelocatorSubid
  - rd_firstRelfilelocatorSubid
  - rd_droppedSubid
- Called from:
  - [AtEOSubXact_RelationCache](AtEOSubXact_RelationCache.md) (in two different code paths)

## Notes and Other Information
- This is a static (internal) function within relcache.c
- The function must be idempotent to handle potential duplicate entries in the eoxact_list
- Special handling for relations that are both created and dropped within the same subtransaction
- Uses WARNING level logging for leaked references to avoid error-during-error-recovery loops
- The function handles four different subtransaction ID fields, each representing different aspects of relation lifecycle within subtransactions
- When transferring ownership to parent subtransaction, the function preserves the relation state for potential cleanup at higher transaction levels
- Safe removal of relcache entries only occurs when reference counts are zero to prevent dangling pointers