# ReindexMultipleInternal

## Location
[src/backend/commands/indexcmds.c:3311-3436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L3311-L3436)

## Overview
ReindexMultipleInternal reindexes a list of relations, with each relation being processed in its own separate transaction to ensure proper isolation and error recovery.

## Definition

```c
typedef struct ReindexIndexInfo
	{
		Oid			indexId;
		Oid			tableId;
		Oid			amId;
		bool		safe;		/* for set_indexsafe_procflags */
	} ReindexIndexInfo;
```
## Detailed Description
This internal function handles the bulk reindexing of multiple database relations by processing each relation in its own transaction context. The function commits the current transaction at the beginning and starts fresh transactions for each relation to be reindexed. This approach ensures that if one relation fails to reindex, it doesn't affect the processing of other relations.

The function performs several key operations for each relation:
- Validates that the relation still exists before attempting to reindex
- Checks permissions for tablespace operations when specified
- Handles different reindexing strategies based on relation type and options (concurrent vs. standard reindexing)
- Provides appropriate verbose output when requested

The function supports both concurrent and standard reindexing modes, automatically choosing the appropriate reindex function based on relation type (index vs. table) and reindexing options.

## Parameters
- `stmt`: ReindexStmt structure containing the original REINDEX statement information
- `relids`: List of relation OIDs to be reindexed
- `params`: ReindexParams structure containing reindexing options and parameters

## Dependencies
- Functions called/Symbols referenced:
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - SearchSysCacheExists1
  - [object_aclcheck](../o/object_aclcheck.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [get_rel_persistence](../g/get_rel_persistence.md)
  - ReindexRelationConcurrently
  - [reindex_index](../r/reindex_index.md)
  - [reindex_relation](../r/reindex_relation.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [get_rel_name](../g/get_rel_name.md)
- Called from (representative examples):
  - [ReindexMultipleTables](ReindexMultipleTables.md)
  - [ReindexPartitions](ReindexPartitions.md)

## Notes and Other Information
- The function is static (internal to indexcmds.c) and designed for internal use within the reindexing subsystem
- Each relation is processed in its own transaction to provide isolation and error recovery
- The function handles partitioned relations by asserting they should never be processed directly (their leaves should be built first)
- Supports both concurrent and standard reindexing modes with appropriate option handling
- Includes proper permission checking for tablespace operations
- Provides verbose output capability for monitoring reindex operations
- Uses REINDEXOPT_MISSING_OK flag to handle relations that may have been dropped during processing

## Simplified Source

```c
static void
ReindexMultipleInternal(const ReindexStmt *stmt, const List *relids, const ReindexParams *params)
{
    ListCell *l;

    // Commit current transaction and start fresh transactions for each relation
    PopActiveSnapshot();
    CommitTransactionCommand();

    foreach(l, relids)
    {
        Oid relid = lfirst_oid(l);
        char relkind;
        char relpersistence;

        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        // Skip if relation no longer exists
        if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
        {
            PopActiveSnapshot();
            CommitTransactionCommand();
            continue;
        }

        // Check tablespace permissions if specified
        if (OidIsValid(params->tablespaceOid) &&
            params->tablespaceOid != MyDatabaseTableSpace)
        {
            AclResult aclresult = object_aclcheck(TableSpaceRelationId, params->tablespaceOid,
                                                  GetUserId(), ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, OBJECT_TABLESPACE,
                               get_tablespace_name(params->tablespaceOid));
        }

        relkind = get_rel_relkind(relid);
        relpersistence = get_rel_persistence(relid);

        // Partitioned relations should never be processed directly
        Assert(!RELKIND_HAS_PARTITIONS(relkind));

        // Choose reindex method based on concurrent flag and relation type
        if ((params->options & REINDEXOPT_CONCURRENTLY) != 0 &&
            relpersistence != RELPERSISTENCE_TEMP)
        {
            // Concurrent reindex
            ReindexParams newparams = *params;
            newparams.options |= REINDEXOPT_MISSING_OK;
            (void) ReindexRelationConcurrently(stmt, relid, &newparams);
            if (ActiveSnapshotSet())
                PopActiveSnapshot();
        }
        else if (relkind == RELKIND_INDEX)
        {
            // Standard index reindex
            ReindexParams newparams = *params;
            newparams.options |= REINDEXOPT_REPORT_PROGRESS | REINDEXOPT_MISSING_OK;
            reindex_index(stmt, relid, false, relpersistence, &newparams);
            PopActiveSnapshot();
        }
        else
        {
            // Standard table reindex
            bool result;
            ReindexParams newparams = *params;
            newparams.options |= REINDEXOPT_REPORT_PROGRESS | REINDEXOPT_MISSING_OK;
            result = reindex_relation(stmt, relid,
                                      REINDEX_REL_PROCESS_TOAST | REINDEX_REL_CHECK_CONSTRAINTS,
                                      &newparams);

            // Verbose output for successful table reindex
            if (result && (params->options & REINDEXOPT_VERBOSE) != 0)
                ereport(INFO, (errmsg("table \"%s.%s\" was reindexed",
                                      get_namespace_name(get_rel_namespace(relid)),
                                      get_rel_name(relid))));

            PopActiveSnapshot();
        }

        CommitTransactionCommand();
    }

    StartTransactionCommand();
}
```