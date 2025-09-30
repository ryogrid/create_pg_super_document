# reindex_index

## Location
[src/backend/catalog/index.c:3547-3886](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L3547-L3886)

## Overview
The  function recreates a single index from scratch, handling all aspects of index reconstruction including validation, locking, storage management, and constraint checking.

## Definition

```c
enumber(iRel, persistence);
```
## Detailed Description
This function performs a complete reconstruction of a single index specified by its OID. It handles the complex process of safely rebuilding an index while maintaining database consistency and transaction safety. The function manages security contexts, progress reporting, tablespace changes, and constraint validation. It supports both regular and "missing ok" modes for handling potentially non-existent relations.

The reindexing process involves:
1. Opening and locking the parent heap relation
2. Setting up proper security context and GUC settings
3. Opening and exclusively locking the target index
4. Validating that the index can be reindexed (checking for partitioned indexes, temp tables, etc.)
5. Optionally moving the index to a new tablespace
6. Suppressing index usage during reconstruction
7. Creating new physical storage and rebuilding the index
8. Updating index validity flags in the system catalog
9. Cleaning up locks and security context

## Parameters / Member Variables
- : Optional REINDEX statement for event trigger collection; can be NULL
- : Object identifier of the index to be reindexed
- : If true, skips uniqueness and exclusion constraint validation during rebuild
- : Storage persistence type for the new index files
- : Reindex parameters including options for progress reporting, verbosity, missing ok behavior, and target tablespace

## Dependencies
- Functions called/Symbols referenced:
  - [IndexGetRelation](../I/IndexGetRelation.md): Gets heap relation OID from index OID
  - [table_open](../t/table_open.md)/try_table_open: Opens the parent heap relation
  - [index_open](../i/index_open.md)/try_index_open: Opens the target index relation
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md): Manages security context for index functions
  - [CheckRelationTableSpaceMove](../C/CheckRelationTableSpaceMove.md): Validates tablespace move operations
  - [CheckTableNotInUse](../C/CheckTableNotInUse.md): Ensures no active usage of the index
  - [SetRelationTableSpace](../S/SetRelationTableSpace.md): Updates tablespace information
  - [RelationDropStorage](../R/RelationDropStorage.md): Schedules old storage file deletion
  - [SetReindexProcessing](../S/SetReindexProcessing.md)/ResetReindexProcessing: Controls index usage suppression
  - [index_build](../i/index_build.md): Performs the actual index reconstruction
  - [BuildIndexInfo](../B/BuildIndexInfo.md): Creates index metadata structure
- Called from (representative examples):
  - [reindex_relation](reindex_relation.md): For reindexing all indexes of a relation
  - [ReindexIndex](../R/ReindexIndex.md): Direct command interface for REINDEX INDEX
  - [ReindexMultipleInternal](../R/ReindexMultipleInternal.md): Batch reindexing operations

## Notes and Other Information
- Requires AccessExclusiveLock on the target index to prevent concurrent access
- Automatically promotes predicate locks to heap relation level during reconstruction
- Supports progress reporting through the PostgreSQL statistics collector
- Can handle invalid indexes from failed CREATE INDEX CONCURRENTLY operations
- Prevents reindexing of partitioned indexes (which have no physical storage)
- Includes comprehensive error checking for temporary tables and TOAST indexes
- Manages transaction-level GUC changes and security context properly
- Updates system catalogs to mark rebuilt indexes as valid and ready

## Simplified Source

```c
void
reindex_index(const ReindexStmt *stmt, Oid indexId,
              bool skip_constraint_checks, char persistence,
              const ReindexParams *params)
{
    Relation iRel, heapRelation;
    Oid heapId;
    Oid save_userid;
    int save_sec_context, save_nestlevel;
    IndexInfo *indexInfo;
    bool skipped_constraint = false;
    bool progress = ((params->options & REINDEXOPT_REPORT_PROGRESS) != 0);
    bool set_tablespace = false;

    // Open and lock the parent heap relation
    heapId = IndexGetRelation(indexId, (params->options & REINDEXOPT_MISSING_OK) != 0);
    if (!OidIsValid(heapId))
        return;

    heapRelation = (params->options & REINDEXOPT_MISSING_OK) != 0 ?
                   try_table_open(heapId, ShareLock) :
                   table_open(heapId, ShareLock);
    if (!heapRelation)
        return;

    // Switch to table owner's userid for security
    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    SetUserIdAndSecContext(heapRelation->rd_rel->relowner,
                          save_sec_context | SECURITY_RESTRICTED_OPERATION);
    save_nestlevel = NewGUCNestLevel();
    RestrictSearchPath();

    // Set up progress reporting if requested
    if (progress) {
        pgstat_progress_start_command(PROGRESS_COMMAND_CREATE_INDEX, heapId);
        // Set progress parameters
    }

    // Open and lock the target index
    iRel = (params->options & REINDEXOPT_MISSING_OK) != 0 ?
           try_index_open(indexId, AccessExclusiveLock) :
           index_open(indexId, AccessExclusiveLock);
    if (!iRel) {
        // Cleanup and return
        AtEOXact_GUC(false, save_nestlevel);
        SetUserIdAndSecContext(save_userid, save_sec_context);
        table_close(heapRelation, NoLock);
        return;
    }

    // Validation checks
    if (iRel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
        elog(ERROR, "cannot reindex partitioned index");
    if (RELATION_IS_OTHER_TEMP(iRel))
        ereport(ERROR, (errmsg("cannot reindex temporary tables of other sessions")));

    // Check for tablespace move
    if (OidIsValid(params->tablespaceOid) &&
        CheckRelationTableSpaceMove(iRel, params->tablespaceOid))
        set_tablespace = true;

    // Ensure index is not in use
    CheckTableNotInUse(iRel, "REINDEX INDEX");

    // Handle tablespace change if needed
    if (set_tablespace) {
        SetRelationTableSpace(iRel, params->tablespaceOid, InvalidOid);
        RelationDropStorage(iRel);
        RelationAssumeNewRelfilelocator(iRel);
        CommandCounterIncrement();
    }

    // Prepare for index rebuild
    TransferPredicateLocksToHeapRelation(iRel);
    indexInfo = BuildIndexInfo(iRel);

    // Skip constraint checks if requested
    if (skip_constraint_checks) {
        if (indexInfo->ii_Unique || indexInfo->ii_ExclusionOps != NULL)
            skipped_constraint = true;
        indexInfo->ii_Unique = false;
        indexInfo->ii_ExclusionOps = NULL;
    }

    // Suppress index usage during rebuild
    SetReindexProcessing(heapId, indexId);

    // Create new physical storage and rebuild
    RelationSetNewRelfilenumber(iRel, persistence);
    index_build(heapRelation, iRel, indexInfo, true, true);

    // Re-allow index usage
    ResetReindexProcessing();

    // Update index validity flags if constraints weren't skipped
    if (!skipped_constraint) {
        // Update pg_index to mark index as valid/ready/live
    }

    // Cleanup
    AtEOXact_GUC(false, save_nestlevel);
    SetUserIdAndSecContext(save_userid, save_sec_context);
    index_close(iRel, NoLock);
    table_close(heapRelation, NoLock);

    if (progress)
        pgstat_progress_end_command();
}
```