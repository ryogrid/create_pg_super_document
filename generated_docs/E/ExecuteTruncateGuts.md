# ExecuteTruncateGuts

## Location
[src/backend/commands/tablecmds.c:1915-2301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L1915-L2301)

## Overview
ExecuteTruncateGuts implements the core TRUNCATE logic, handling the actual data deletion, foreign key cascade processing, sequence restarting, trigger execution, and WAL logging for both direct TRUNCATE commands and logical replication.

## Definition
```c
void ExecuteTruncateGuts(List *explicit_rels, List *relids, List *relids_logged, 
                        DropBehavior behavior, bool restart_seqs, bool run_as_table_owner)
```

## Detailed Description
This function performs the internal implementation of TRUNCATE operations with comprehensive functionality:

1. **CASCADE Processing**: In CASCADE mode, iteratively finds and includes all tables with foreign key references to the target tables, acquiring locks and performing checks on each newly discovered table
2. **Foreign Key Validation**: Validates that all foreign key constraints are satisfied based on the specified behavior (CASCADE/RESTRICT)
3. **Sequence Handling**: When restart_seqs is true, finds all owned sequences and validates permissions before restarting them
4. **Trigger Management**: Sets up executor state and fires BEFORE STATEMENT TRUNCATE triggers before the actual truncation
5. **Table Truncation**: Implements two truncation strategies:
   - **Fast Path**: For tables created in the current subtransaction, uses immediate non-rollbackable truncation
   - **Safe Path**: For existing tables, creates new storage files and schedules old files for deletion at commit
6. **Foreign Table Support**: Groups foreign tables by server and delegates to FDW-specific truncation routines
7. **Index Rebuilding**: Reconstructs indexes after truncation to maintain consistency
8. **WAL Logging**: Creates WAL records for logical decoding when needed
9. **AFTER Trigger Processing**: Fires AFTER STATEMENT TRUNCATE triggers after successful truncation
10. **Cleanup**: Properly closes relations and cleans up resources

The function handles both regular tables and foreign tables, supports inheritance hierarchies, and ensures transactional safety through careful resource management.

## Parameters / Member Variables
- `explicit_rels`: List of Relation objects explicitly specified in the TRUNCATE command
- `relids`: List of OIDs corresponding to explicit_rels
- `relids_logged`: Subset of relids that require WAL logging for logical decoding
- `behavior`: DROP_CASCADE or DROP_RESTRICT behavior for foreign key handling
- `restart_seqs`: Boolean indicating whether to restart sequences owned by truncated tables
- `run_as_table_owner`: Boolean indicating whether triggers should run with table owner privileges

## Dependencies
- Functions called/Symbols referenced:
  - [heap_truncate_find_FKs](../h/heap_truncate_find_FKs.md)
  - [heap_truncate_check_FKs](../h/heap_truncate_check_FKs.md)
  - [truncate_check_rel](../t/truncate_check_rel.md)
  - [truncate_check_perms](../t/truncate_check_perms.md)
  - [truncate_check_activity](../t/truncate_check_activity.md)
  - [getOwnedSequences](../g/getOwnedSequences.md)
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - [ExecBSTruncateTriggers](ExecBSTruncateTriggers.md)
  - [ExecASTruncateTriggers](ExecASTruncateTriggers.md)
  - [heap_truncate_one_rel](../h/heap_truncate_one_rel.md)
  - [RelationSetNewRelfilenumber](../R/RelationSetNewRelfilenumber.md)
  - [reindex_relation](../r/reindex_relation.md)
  - [ResetSequence](../R/ResetSequence.md)
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
- Called from (representative examples):
  - [ExecuteTruncate](ExecuteTruncate.md)
  - [apply_handle_truncate](../a/apply_handle_truncate.md)

## Notes and Other Information
- This function is used both by direct TRUNCATE commands and logical replication subscribers
- The function implements PostgreSQL's two-phase truncation approach: fast path for new tables, safe path for existing tables
- Foreign table truncation is delegated to FDW callbacks, allowing different data sources to implement their own truncation logic
- WAL logging is conditional and only occurs when logical decoding is active and relations require it
- The function maintains transactional safety by using subtransaction IDs to determine the appropriate truncation strategy
- Sequence restarting permissions are checked early to avoid partial execution failures
- [Trigger](../T/Trigger.md) execution can optionally run with table owner privileges for security purposes

## Simplified Source

```c
void ExecuteTruncateGuts(List *explicit_rels, List *relids, List *relids_logged,
                        DropBehavior behavior, bool restart_seqs, bool run_as_table_owner) {
    List *rels = list_copy(explicit_rels);
    List *seq_relids = NIL;
    HTAB *ft_htab = NULL;
    EState *estate;
    ResultRelInfo *resultRelInfos;

    // CASCADE mode: find all referenced tables through foreign keys
    if (behavior == DROP_CASCADE) {
        for (;;) {
            List *newrelids = heap_truncate_find_FKs(relids);
            if (newrelids == NIL)
                break;  // No more dependencies found

            foreach(cell, newrelids) {
                Oid relid = lfirst_oid(cell);
                Relation rel = table_open(relid, AccessExclusiveLock);

                // Validate permissions and activity
                truncate_check_rel(relid, rel->rd_rel);
                truncate_check_perms(relid, rel->rd_rel);
                truncate_check_activity(rel);

                // Add to processing lists
                rels = lappend(rels, rel);
                relids = lappend_oid(relids, relid);
                if (RelationIsLogicallyLogged(rel))
                    relids_logged = lappend_oid(relids_logged, relid);
            }
        }
    }

    // Validate foreign key constraints
    if (behavior == DROP_RESTRICT)
        heap_truncate_check_FKs(rels, false);

    // Handle sequence restart: lock sequences and check permissions
    if (restart_seqs) {
        foreach(cell, rels) {
            Relation rel = (Relation) lfirst(cell);
            List *seqlist = getOwnedSequences(RelationGetRelid(rel));

            foreach(seqcell, seqlist) {
                Oid seq_relid = lfirst_oid(seqcell);
                Relation seq_rel = relation_open(seq_relid, AccessExclusiveLock);

                // Check ownership permissions
                if (!object_ownercheck(RelationRelationId, seq_relid, GetUserId()))
                    aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SEQUENCE,
                                 RelationGetRelationName(seq_rel));

                seq_relids = lappend_oid(seq_relids, seq_relid);
                relation_close(seq_rel, NoLock);
            }
        }
    }

    // Setup trigger execution environment
    AfterTriggerBeginQuery();
    estate = CreateExecutorState();
    resultRelInfos = (ResultRelInfo *) palloc(list_length(rels) * sizeof(ResultRelInfo));

    // Initialize result relation info for each table
    foreach(cell, rels) {
        Relation rel = (Relation) lfirst(cell);
        InitResultRelInfo(resultRelInfo, rel, 0, NULL, 0);
        estate->es_opened_result_relations = lappend(estate->es_opened_result_relations, resultRelInfo);
        resultRelInfo++;
    }

    // Execute BEFORE STATEMENT TRUNCATE triggers
    resultRelInfo = resultRelInfos;
    foreach(cell, rels) {
        if (run_as_table_owner)
            SwitchToUntrustedUser(resultRelInfo->ri_RelationDesc->rd_rel->relowner, &ucxt);
        ExecBSTruncateTriggers(estate, resultRelInfo);
        if (run_as_table_owner)
            RestoreUserContext(&ucxt);
        resultRelInfo++;
    }

    // Truncate each table
    SubTransactionId mySubid = GetCurrentSubTransactionId();
    foreach(cell, rels) {
        Relation rel = (Relation) lfirst(cell);

        // Skip partitioned tables
        if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
            continue;

        // Handle foreign tables through FDW callbacks
        if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
            // Group foreign tables by server for bulk operations
            Oid serverid = GetForeignServerIdByRelId(RelationGetRelid(rel));
            // Add to hash table for later processing
            continue;
        }

        // Choose truncation strategy based on transaction context
        if (rel->rd_createSubid == mySubid || rel->rd_newRelfilelocatorSubid == mySubid) {
            // Fast path: immediate truncation for new tables
            heap_truncate_one_rel(rel);
        } else {
            // Safe path: create new storage files
            CheckTableForSerializableConflictIn(rel);
            RelationSetNewRelfilenumber(rel, rel->rd_rel->relpersistence);

            // Handle toast table if exists
            Oid toast_relid = rel->rd_rel->reltoastrelid;
            if (OidIsValid(toast_relid)) {
                Relation toastrel = relation_open(toast_relid, AccessExclusiveLock);
                RelationSetNewRelfilenumber(toastrel, toastrel->rd_rel->relpersistence);
                table_close(toastrel, NoLock);
            }

            // Rebuild indexes
            reindex_relation(NULL, RelationGetRelid(rel), REINDEX_REL_PROCESS_TOAST, &reindex_params);
        }

        pgstat_count_truncate(rel);
    }

    // Process foreign tables through FDW callbacks
    if (ft_htab) {
        hash_seq_init(&seq, ft_htab);
        while ((ft_info = hash_seq_search(&seq)) != NULL) {
            FdwRoutine *routine = GetFdwRoutineByServerId(ft_info->serverid);
            routine->ExecForeignTruncate(ft_info->rels, behavior, restart_seqs);
        }
        hash_destroy(ft_htab);
    }

    // Restart sequences if requested
    foreach(cell, seq_relids) {
        Oid seq_relid = lfirst_oid(cell);
        ResetSequence(seq_relid);
    }

    // Create WAL record for logical decoding
    if (relids_logged != NIL) {
        xl_heap_truncate xlrec;
        Oid *logrelids = palloc(list_length(relids_logged) * sizeof(Oid));

        // Setup WAL record
        xlrec.dbId = MyDatabaseId;
        xlrec.nrelids = list_length(relids_logged);
        xlrec.flags = 0;
        if (behavior == DROP_CASCADE)
            xlrec.flags |= XLH_TRUNCATE_CASCADE;
        if (restart_seqs)
            xlrec.flags |= XLH_TRUNCATE_RESTART_SEQS;

        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfHeapTruncate);
        XLogRegisterData((char *) logrelids, list_length(relids_logged) * sizeof(Oid));
        XLogInsert(RM_HEAP_ID, XLOG_HEAP_TRUNCATE);
    }

    // Execute AFTER STATEMENT TRUNCATE triggers
    resultRelInfo = resultRelInfos;
    foreach(cell, rels) {
        if (run_as_table_owner)
            SwitchToUntrustedUser(resultRelInfo->ri_RelationDesc->rd_rel->relowner, &ucxt);
        ExecASTruncateTriggers(estate, resultRelInfo);
        if (run_as_table_owner)
            RestoreUserContext(&ucxt);
        resultRelInfo++;
    }

    // Cleanup
    AfterTriggerEndQuery(estate);
    FreeExecutorState(estate);

    // Close cascade-opened relations
    rels = list_difference_ptr(rels, explicit_rels);
    foreach(cell, rels) {
        Relation rel = (Relation) lfirst(cell);
        table_close(rel, NoLock);
    }
}
```