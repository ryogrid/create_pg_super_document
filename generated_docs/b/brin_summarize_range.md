# brin_summarize_range

## Location
[src/backend/access/brin/brin.c:1371-1481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1371-L1481)

## Overview
A SQL-callable function that summarizes a specific page range in a BRIN index, or all unsummarized ranges if the special value BRIN_ALL_BLOCKRANGES is provided.

## Definition

```c
Datum
brin_summarize_range(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides the core functionality for BRIN index maintenance by creating or updating summary tuples for specified block ranges. It performs comprehensive validation including checking that the target is a valid BRIN index, ensuring proper permissions, and verifying that recovery is not in progress. The function implements proper locking protocols (table before index to avoid deadlocks) and security context switching for autovacuum operations. When the special value BRIN_ALL_BLOCKRANGES is passed, it processes all unsummarized ranges in the index.

## Parameters / Member Variables
- : The OID of the BRIN index to be summarized (accessed via )
- : The block number to summarize, or BRIN_ALL_BLOCKRANGES for all blocks (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if database recovery is ongoing
  - : Gets the heap relation OID from index OID
  - /: Opens relations with appropriate locks
  - /: Manages security context for autovacuum
  - /: Security restrictions
  - : Verifies ownership permissions
  - : Performs the actual summarization work
  - : Rolls back GUC changes
  - : Closes relations and releases locks
  - : Special constant for processing all ranges
  - : Lock level used for operations
- Called from (representative examples):
  - : Wrapper function for SQL interface
  - : Autovacuum worker process

## Notes and Other Information
- Blocks operation during recovery with a specific error message about BRIN control functions
- Implements proper deadlock avoidance by locking table before index
- Switches to table owner's userid when called by autovacuum for security
- Validates that the target relation is actually a BRIN index (relam == BRIN_AM_OID)
- Requires table ownership for execution (similar to VACUUM privileges)
- Returns the number of summarized ranges as an integer
- Only processes valid indexes (indisvalid must be true)
- Handles race conditions by rechecking index-to-table mapping after acquiring locks

## Simplified Source

```c
Datum
brin_summarize_range(PG_FUNCTION_ARGS)
{
    Oid indexoid = PG_GETARG_OID(0);
    int64 heapBlk64 = PG_GETARG_INT64(1);
    BlockNumber heapBlk;
    Relation indexRel, heapRel;
    Oid heapoid, save_userid;
    int save_sec_context, save_nestlevel;
    double numSummarized = 0;

    // Block operation during recovery
    if (RecoveryInProgress())
        ereport(ERROR, ...);

    // Validate block number range
    if (heapBlk64 > BRIN_ALL_BLOCKRANGES || heapBlk64 < 0)
        ereport(ERROR, ...);
    heapBlk = (BlockNumber) heapBlk64;

    // Get heap relation and handle security context for autovacuum
    heapoid = IndexGetRelation(indexoid, true);
    if (OidIsValid(heapoid)) {
        heapRel = table_open(heapoid, ShareUpdateExclusiveLock);
        GetUserIdAndSecContext(&save_userid, &save_sec_context);
        SetUserIdAndSecContext(heapRel->rd_rel->relowner,
                               save_sec_context | SECURITY_RESTRICTED_OPERATION);
        save_nestlevel = NewGUCNestLevel();
        RestrictSearchPath();
    } else {
        heapRel = NULL;
    }

    // Open index and validate it's a BRIN index
    indexRel = index_open(indexoid, ShareUpdateExclusiveLock);
    if (indexRel->rd_rel->relkind != RELKIND_INDEX ||
        indexRel->rd_rel->relam != BRIN_AM_OID)
        ereport(ERROR, ...);

    // Check ownership permissions
    if (heapRel != NULL && !object_ownercheck(RelationRelationId, indexoid, save_userid))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_INDEX, ...);

    // Perform summarization if index is valid
    if (indexRel->rd_index->indisvalid)
        brinsummarize(indexRel, heapRel, heapBlk, true, &numSummarized, NULL);

    // Cleanup: restore security context and close relations
    AtEOXact_GUC(false, save_nestlevel);
    SetUserIdAndSecContext(save_userid, save_sec_context);
    relation_close(indexRel, ShareUpdateExclusiveLock);
    relation_close(heapRel, ShareUpdateExclusiveLock);

    PG_RETURN_INT32((int32) numSummarized);
}
```