# DefineIndex

## Location
[src/backend/commands/indexcmds.c:540-1791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L540-L1791)

## Overview
Creates a new index on a specified table relation, handling both regular and concurrent index creation with comprehensive validation, permission checks, and support for partitioned tables.

## Definition
```c
ObjectAddress DefineIndex(Oid tableId, IndexStmt *stmt, Oid indexRelationId, Oid parentIndexId, Oid parentConstraintId, int total_parts, bool is_alter_table, bool check_rights, bool check_not_in_use, bool skip_build, bool quiet)
```

## Detailed Description
DefineIndex is the primary function responsible for creating indexes in PostgreSQL. It manages a complex workflow that includes access method validation, permission checking, attribute processing, constraint handling, and the actual index creation process. The function supports both regular and concurrent index creation modes.

The function manages user identity and security context carefully, especially for pg_dump compatibility - using the table owner userid for most ACL checks while preserving the original userid for predictable ACL checks. This addresses the complexity of running opaque expressions (like function calls) safely during index creation.

Key responsibilities include:
- Validating the access method and its capabilities
- Processing index attributes, including key columns and INCLUDE columns
- Handling partitioned table constraints and validation
- Managing concurrent vs. non-concurrent build modes
- Creating catalog entries and optionally building the index
- Supporting primary key, unique, and exclusion constraints
- Managing progress reporting for long-running operations

For concurrent builds, the function implements a multi-phase process:
1. Create catalog entries with index marked as not ready for inserts
2. Wait for existing transactions to complete
3. Build the index while allowing concurrent DML
4. Wait again for snapshot consistency
5. Validate the index and mark it as ready

## Parameters / Member Variables
- `tableId`: OID of the table relation on which the index is to be created
- `stmt`: IndexStmt describing the properties of the new index
- `indexRelationId`: Normally InvalidOid, but can specify a preselected OID during bootstrap
- `parentIndexId`: OID of the parent index; InvalidOid if not a child of a partitioned index
- `parentConstraintId`: OID of the parent constraint; InvalidOid if not a constraint child
- `total_parts`: Total number of direct and indirect partitions; -1 if unknown or not partitioned
- `is_alter_table`: Boolean indicating this is due to ALTER rather than CREATE operation
- `check_rights`: Whether to check CREATE rights in namespace and tablespace
- `check_not_in_use`: Whether to check that table is not in use in current session
- `skip_build`: Whether to make catalog entries but skip building index files
- `quiet`: Whether to suppress NOTICE messages for constraints

## Dependencies
- Functions called/Symbols referenced:
  - [CheckTableNotInUse](../C/CheckTableNotInUse.md)
  - [CheckPredicate](../C/CheckPredicate.md)
  - [ChooseIndexName](../C/ChooseIndexName.md)
  - [ChooseIndexColumnNames](../C/ChooseIndexColumnNames.md)
  - [ComputeIndexAttrs](../C/ComputeIndexAttrs.md)
  - [GetIndexAmRoutine](../G/GetIndexAmRoutine.md)
  - [WaitForOlderSnapshots](../W/WaitForOlderSnapshots.md)
  - [index_create](../i/index_create.md)
  - [index_concurrently_build](../i/index_concurrently_build.md)
  - [validate_index](../v/validate_index.md)
  - [CreateComments](../C/CreateComments.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)
  - [DefineRelation](DefineRelation.md)
  - [ATExecAddIndex](../A/ATExecAddIndex.md)
  - [AttachPartitionEnsureIndexes](../A/AttachPartitionEnsureIndexes.md)

## Notes and Other Information
- Supports both regular and concurrent index creation modes
- Handles complex partitioned table scenarios with recursive index creation on partitions
- Implements comprehensive validation for unique constraints on partitioned tables
- Manages GUC variables and security context carefully during execution
- Progress reporting is integrated for monitoring long-running operations
- Contains special handling for obsolete RTREE access method (converts to GiST)
- Validates that partition keys are included in unique/exclusion constraints for partitioned tables
- Located in src/backend/commands/indexcmds.c:540-1791

## Simplified Source

```c
ObjectAddress
DefineIndex(Oid tableId, IndexStmt *stmt, Oid indexRelationId,
           Oid parentIndexId, Oid parentConstraintId, int total_parts,
           bool is_alter_table, bool check_rights, bool check_not_in_use,
           bool skip_build, bool quiet)
{
    bool concurrent;
    char *indexRelationName;
    char *accessMethodName;
    Oid accessMethodId;
    Oid namespaceId;
    Oid tablespaceId;
    Relation rel;
    IndexInfo *indexInfo;
    ObjectAddress address;

    // Setup: Configure security context and GUC settings
    RestrictSearchPath();
    if (stmt->reset_default_tblspc)
        set_config_option("default_tablespace", "", ...);

    // Determine build mode: Force non-concurrent for temp relations
    concurrent = stmt->concurrent &&
                 get_rel_persistence(tableId) != RELPERSISTENCE_TEMP;

    // Progress reporting setup
    if (!OidIsValid(parentIndexId))
        pgstat_progress_start_command(PROGRESS_COMMAND_CREATE_INDEX, tableId);

    // Validate column counts
    int numberOfKeyAttributes = list_length(stmt->indexParams);
    int numberOfAttributes = list_length(allIndexParams);
    if (numberOfKeyAttributes <= 0 || numberOfAttributes > INDEX_MAX_KEYS)
        ereport(ERROR, ...);

    // Open and lock the table
    LockMode lockmode = concurrent ? ShareUpdateExclusiveLock : ShareLock;
    rel = table_open(tableId, lockmode);

    // Security: Switch to table owner's userid
    GetUserIdAndSecContext(&root_save_userid, &root_save_sec_context);
    SetUserIdAndSecContext(rel->rd_rel->relowner,
                          root_save_sec_context | SECURITY_RESTRICTED_OPERATION);

    // Validate relation kind (tables, matviews, partitioned tables only)
    switch (rel->rd_rel->relkind) {
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
        case RELKIND_PARTITIONED_TABLE:
            break;
        default:
            ereport(ERROR, ...);
    }

    // Handle partitioned tables
    bool partitioned = rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE;
    if (partitioned && stmt->concurrent)
        ereport(ERROR, ...); // No concurrent builds on partitioned tables

    // Permission checks
    if (check_rights && !IsBootstrapProcessingMode())
        check_namespace_and_tablespace_permissions();

    // Select tablespace
    if (stmt->tableSpace)
        tablespaceId = get_tablespace_oid(stmt->tableSpace, false);
    else
        tablespaceId = GetDefaultTablespace(rel->rd_rel->relpersistence, partitioned);

    // Choose index name if not specified
    if (!stmt->idxname)
        indexRelationName = ChooseIndexName(...);

    // Validate and setup access method
    accessMethodName = stmt->accessMethod;
    validate_access_method_capabilities(accessMethodName, stmt);

    // Process index attributes and expressions
    indexInfo = makeIndexInfo(...);
    ComputeIndexAttrs(indexInfo, typeIds, collationIds, opclassIds, ...);

    // Special validation for PRIMARY KEY indexes
    if (stmt->primary)
        index_check_primary_key(rel, indexInfo, is_alter_table, stmt);

    // Validate unique constraints on partitioned tables
    if (partitioned && (stmt->unique || stmt->excludeOpNames))
        validate_partition_key_in_unique_constraint(...);

    // Create the index
    if (!concurrent) {
        // Standard index creation
        indexRelationId = index_create(rel, indexRelationName, ...);

        // Handle partitioned table recursion
        if (partitioned)
            create_indexes_on_partitions();

        table_close(rel, NoLock);
        return address;
    }

    // Concurrent index creation (multi-phase process)

    // Phase 1: Create catalog entries, mark as not ready
    indexRelationId = index_create(rel, indexRelationName, ...,
                                  INDEX_CREATE_SKIP_BUILD);

    // Get session lock and commit to make index visible
    LockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
    CommitTransactionCommand();
    StartTransactionCommand();

    // Phase 2: Wait for existing transactions, then build index
    WaitForLockers(heaplocktag, ShareLock, true);
    PushActiveSnapshot(GetTransactionSnapshot());
    index_concurrently_build(tableId, indexRelationId);
    PopActiveSnapshot();

    // Phase 3: Commit and wait again for snapshot consistency
    CommitTransactionCommand();
    StartTransactionCommand();
    WaitForLockers(heaplocktag, ShareLock, true);

    // Phase 4: Validate index and mark as ready
    snapshot = RegisterSnapshot(GetTransactionSnapshot());
    validate_index(tableId, indexRelationId, snapshot);
    UnregisterSnapshot(snapshot);

    // Final phase: Wait for older snapshots and mark valid
    CommitTransactionCommand();
    StartTransactionCommand();
    WaitForOlderSnapshots(limitXmin, true);
    index_set_state_flags(indexRelationId, INDEX_CREATE_SET_VALID);

    // Cleanup and release session lock
    UnlockRelationIdForSession(&heaprelid, ShareUpdateExclusiveLock);
    pgstat_progress_end_command();

    return address;
}
```