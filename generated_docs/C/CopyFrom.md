# CopyFrom

## Location
[src/backend/commands/copyfrom.c:628-1367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfrom.c#L628-L1367)

## Overview
Executes the COPY FROM operation to transfer data from a file or data source into a PostgreSQL relation, handling parsing, validation, constraint checking, and tuple insertion with performance optimizations.

## Definition
```c
uint64
CopyFrom(CopyFromState cstate)
```

## Detailed Description
CopyFrom is the main execution engine for COPY FROM operations in PostgreSQL. It reads tuples from the configured data source (file, stdin, etc.), validates and transforms them according to table constraints and triggers, and inserts them into the target relation. The function implements several optimization strategies including multi-insert buffering for improved performance, partition tuple routing for partitioned tables, and specialized handling for foreign tables.

The function supports various advanced features including:
- COPY FREEZE optimization for new tables to avoid WAL overhead
- Multi-insert buffering to reduce individual insert costs
- Partition tuple routing for partitioned tables  
- Error handling modes (STOP, IGNORE) for data quality issues
- WHERE clause filtering to selectively import data
- Trigger execution (BEFORE/AFTER INSERT, INSTEAD OF INSERT)
- Constraint validation and partition constraint checking
- Foreign data wrapper integration for foreign tables

## Parameters / Member Variables
- `cstate`: CopyFromState containing all configuration, parsing state, target relation, and execution context for the COPY operation

## Dependencies
- Functions called/Symbols referenced:
  - [CreateExecutorState](CreateExecutorState.md)
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)  
  - [ExecInitRangeTable](../E/ExecInitRangeTable.md)
  - [ExecInitResultRelation](../E/ExecInitResultRelation.md)
  - [CheckValidResultRel](CheckValidResultRel.md)
  - [ExecOpenIndices](../E/ExecOpenIndices.md)
  - [MakeTransitionCaptureState](../M/MakeTransitionCaptureState.md)
  - [ExecSetupPartitionTupleRouting](../E/ExecSetupPartitionTupleRouting.md)
  - [CopyMultiInsertInfoInit](CopyMultiInsertInfoInit.md)
  - [GetBulkInsertState](../G/GetBulkInsertState.md)
  - [table_slot_create](../t/table_slot_create.md)
  - [NextCopyFrom](../N/NextCopyFrom.md)
  - [ExecFindPartition](../E/ExecFindPartition.md)
  - [ExecGetRootToChildMap](../E/ExecGetRootToChildMap.md)
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [ExecBRInsertTriggers](../E/ExecBRInsertTriggers.md)
  - [ExecIRInsertTriggers](../E/ExecIRInsertTriggers.md)
  - [ExecComputeStoredGenerated](../E/ExecComputeStoredGenerated.md)
  - [ExecConstraints](../E/ExecConstraints.md)
  - [ExecPartitionCheck](../E/ExecPartitionCheck.md)
  - [CopyMultiInsertInfoNextFreeSlot](CopyMultiInsertInfoNextFreeSlot.md)
  - [CopyMultiInsertInfoStore](CopyMultiInsertInfoStore.md)
  - [CopyMultiInsertInfoFlush](CopyMultiInsertInfoFlush.md)
  - [table_tuple_insert](../t/table_tuple_insert.md)
  - [ExecInsertIndexTuples](../E/ExecInsertIndexTuples.md)
  - [ExecARInsertTriggers](../E/ExecARInsertTriggers.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
- Called from (representative examples):
  - [DoCopy](../D/DoCopy.md) (main entry point for COPY command)
  - [copy_table](../c/copy_table.md) (logical replication)

## Notes and Other Information
- Returns the number of tuples successfully processed and inserted
- Implements three insertion methods: CIM_SINGLE (row-by-row), CIM_MULTI (batched), and CIM_MULTI_CONDITIONAL (dynamic based on partition capabilities)
- Multi-insert optimization is disabled for tables with BEFORE/INSTEAD OF triggers or volatile expressions
- COPY FREEZE requires the table to be created/truncated in the current subtransaction and no prior transaction activity
- Supports ON ERROR IGNORE mode to skip malformed rows rather than aborting the entire operation
- Maintains detailed progress reporting through pgstat_progress_update_param calls
- Handles memory management by switching between per-tuple and query contexts as needed
- Integrates with PostgreSQL's trigger system, constraint checking, and partition pruning mechanisms

## Simplified Source

```c
uint64 CopyFrom(CopyFromState cstate) {
    ResultRelInfo *resultRelInfo;
    EState *estate = CreateExecutorState();
    int64 processed = 0;
    bool has_before_insert_row_trig;
    CopyInsertMethod insertMethod;
    CopyMultiInsertInfo multiInsertInfo;

    // Validate target relation type
    validate_copy_target_relation(cstate->rel);

    // Set up executor state and result relation info
    ExecInitRangeTable(estate, cstate->range_table, cstate->rteperminfos);
    resultRelInfo = setup_result_relation(estate);

    // Configure FREEZE optimization if requested
    if (cstate->opts.freeze) {
        validate_freeze_conditions(cstate);
        ti_options |= TABLE_INSERT_FROZEN;
    }

    // Set up partition routing for partitioned tables
    if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
        proute = ExecSetupPartitionTupleRouting(estate, cstate->rel);
    }

    // Determine insert method (single, multi, or conditional)
    insertMethod = determine_insert_method(cstate, resultRelInfo, proute);

    // Set up multi-insert buffer if using batch mode
    if (insertMethod != CIM_SINGLE) {
        CopyMultiInsertInfoInit(&multiInsertInfo, resultRelInfo, cstate,
                                estate, mycid, ti_options);
    }

    // Execute BEFORE STATEMENT triggers
    ExecBSInsertTriggers(estate, resultRelInfo);

    // Main processing loop
    for (;;) {
        TupleTableSlot *myslot;
        bool skip_tuple = false;

        CHECK_FOR_INTERRUPTS();

        // Read next tuple from input source
        if (!NextCopyFrom(cstate, econtext, myslot->tts_values, myslot->tts_isnull)) {
            break; // End of input
        }

        // Handle soft errors (ON_ERROR modes)
        if (handle_copy_errors(cstate)) {
            continue; // Skip this tuple
        }

        // Apply WHERE clause filtering
        if (cstate->whereClause && !ExecQual(cstate->qualexpr, econtext)) {
            excluded++;
            continue;
        }

        // Handle partition routing for partitioned tables
        if (proute) {
            resultRelInfo = ExecFindPartition(mtstate, target_resultRelInfo,
                                              proute, myslot, estate);
            handle_partition_mapping(myslot, resultRelInfo, map);
        }

        // Execute BEFORE ROW INSERT triggers
        if (has_before_insert_row_trig) {
            if (!ExecBRInsertTriggers(estate, resultRelInfo, myslot)) {
                skip_tuple = true;
            }
        }

        if (!skip_tuple) {
            // Handle INSTEAD OF triggers or regular insertion
            if (has_instead_insert_row_trig) {
                ExecIRInsertTriggers(estate, resultRelInfo, myslot);
            } else {
                // Compute stored generated columns
                compute_stored_generated_columns(resultRelInfo, estate, myslot);

                // Check constraints and partition constraints
                validate_constraints_and_partitions(resultRelInfo, myslot, estate, proute);

                // Insert tuple (either batched or single)
                if (insertMethod == CIM_MULTI || leafpart_use_multi_insert) {
                    // Add to multi-insert buffer
                    CopyMultiInsertInfoStore(&multiInsertInfo, resultRelInfo, myslot,
                                             cstate->line_buf.len, cstate->cur_lineno);

                    // Flush buffer if full
                    if (CopyMultiInsertInfoIsFull(&multiInsertInfo)) {
                        CopyMultiInsertInfoFlush(&multiInsertInfo, resultRelInfo, &processed);
                    }
                } else {
                    // Single tuple insert
                    insert_single_tuple(resultRelInfo, myslot, estate, mycid, ti_options, bistate);

                    // Execute AFTER ROW INSERT triggers
                    ExecARInsertTriggers(estate, resultRelInfo, myslot,
                                         recheckIndexes, cstate->transition_capture);
                }
            }

            processed++;
        }
    }

    // Flush any remaining buffered tuples
    if (insertMethod != CIM_SINGLE && !CopyMultiInsertInfoIsEmpty(&multiInsertInfo)) {
        CopyMultiInsertInfoFlush(&multiInsertInfo, NULL, &processed);
    }

    // Execute AFTER STATEMENT triggers and cleanup
    ExecASInsertTriggers(estate, target_resultRelInfo, cstate->transition_capture);
    cleanup_copy_state(estate, bistate, &multiInsertInfo, proute, mtstate);

    return processed;
}
```