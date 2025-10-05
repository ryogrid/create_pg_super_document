# apply_handle_insert

## Location
[src/backend/replication/logical/worker.c:2373-2463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L2373-L2463)

## Overview
Handles INSERT messages in PostgreSQL logical replication by processing incoming tuple data and inserting it into the appropriate target relation, with support for partitioned tables and proper security context management.

## Definition
```c
static void apply_handle_insert(StringInfo s)
```

## Detailed Description
This function is the main handler for INSERT replication messages in the logical replication worker process. It performs a complete INSERT operation including security context switching, tuple processing, and proper cleanup. The function handles both regular tables and partitioned tables, routing inserts to the correct partition when necessary.

Key operations include:
1. **Message Processing**: Reads the INSERT message data and extracts the relation ID and tuple data
2. **Security Management**: Switches to the table owner's security context unless the subscription is configured to run as the subscription owner
3. **Tuple Processing**: Converts the remote tuple data into a local tuple slot and fills in default values
4. **Execution**: Either directly inserts into a regular table or routes the insert through the partitioning system for partitioned tables
5. **Resource Management**: Properly manages memory contexts, executor state, and relation locks

The function includes comprehensive error handling and ensures proper cleanup of resources regardless of success or failure.

## Parameters / Member Variables
- `s`: StringInfo buffer containing the INSERT message data from the publisher

## Dependencies
- Functions called/Symbols referenced:
  - is_skipping_changes
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)
  - [begin_replication_step](../b/begin_replication_step.md)
  - [logicalrep_read_insert](../l/logicalrep_read_insert.md)
  - [logicalrep_rel_open](../l/logicalrep_rel_open.md)
  - [should_apply_changes_for_rel](../s/should_apply_changes_for_rel.md)
  - [logicalrep_rel_close](../l/logicalrep_rel_close.md)
  - [SwitchToUntrustedUser](../S/SwitchToUntrustedUser.md)
  - [create_edata_for_relation](../c/create_edata_for_relation.md)
  - [ExecInitExtraTupleSlot](../E/ExecInitExtraTupleSlot.md)
  - GetPerTupleMemoryContext
  - [slot_store_data](../s/slot_store_data.md)
  - [slot_fill_defaults](../s/slot_fill_defaults.md)
  - [apply_handle_tuple_routing](apply_handle_tuple_routing.md)
  - [apply_handle_insert_internal](apply_handle_insert_internal.md)
  - [ExecOpenIndices](../E/ExecOpenIndices.md)
  - [ExecCloseIndices](../E/ExecCloseIndices.md)
  - [finish_edata](../f/finish_edata.md)
  - [RestoreUserContext](../R/RestoreUserContext.md)
  - [end_replication_step](../e/end_replication_step.md)
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md) (data structure)
  - [LogicalRepTupleData](../L/LogicalRepTupleData.md) (data structure)
  - [UserContext](../U/UserContext.md) (data structure)
  - [ApplyExecutionData](../A/ApplyExecutionData.md) (data structure)
  - LOGICAL_REP_MSG_INSERT (constant)
  - CMD_INSERT (constant)
- Called from (representative examples):
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- This is a static function within the logical replication worker module
- Supports both regular and partitioned tables with appropriate routing logic
- Implements security context switching based on subscription configuration (runasowner setting)
- Uses PostgreSQL's executor framework for tuple processing and insertion
- Includes comprehensive resource management with proper memory context handling
- Part of the core logical replication message processing pipeline
- Handles early returns for skipped changes and streamed transactions
- Maintains error callback context for better error reporting during replication

## Simplified Source

```c
static void
apply_handle_insert(StringInfo s)
{
    LogicalRepRelMapEntry *rel;
    LogicalRepTupleData newtup;
    LogicalRepRelId relid;
    UserContext ucxt;
    ApplyExecutionData *edata;
    TupleTableSlot *remoteslot;
    bool run_as_owner;

    // Quick exits for skipped changes or streaming transactions
    if (is_skipping_changes() ||
        handle_streamed_transaction(LOGICAL_REP_MSG_INSERT, s))
        return;

    begin_replication_step();

    // Parse INSERT message and open target relation
    relid = logicalrep_read_insert(s, &newtup);
    rel = logicalrep_rel_open(relid, RowExclusiveLock);
    if (!should_apply_changes_for_rel(rel)) {
        logicalrep_rel_close(rel, RowExclusiveLock);
        end_replication_step();
        return;
    }

    // Set up security context
    run_as_owner = MySubscription->runasowner;
    if (!run_as_owner)
        SwitchToUntrustedUser(rel->localrel->rd_rel->relowner, &ucxt);

    // Initialize executor and prepare tuple slot
    edata = create_edata_for_relation(rel);
    remoteslot = ExecInitExtraTupleSlot(edata->estate,
                                        RelationGetDescr(rel->localrel),
                                        &TTSOpsVirtual);

    // Process remote tuple data
    slot_store_data(remoteslot, rel, &newtup);
    slot_fill_defaults(rel, edata->estate, remoteslot);

    // Route to partition or insert directly
    if (rel->localrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        apply_handle_tuple_routing(edata, remoteslot, NULL, CMD_INSERT);
    else {
        ExecOpenIndices(edata->targetRelInfo, false);
        apply_handle_insert_internal(edata, edata->targetRelInfo, remoteslot);
        ExecCloseIndices(edata->targetRelInfo);
    }

    // Cleanup
    finish_edata(edata);
    if (!run_as_owner)
        RestoreUserContext(&ucxt);
    logicalrep_rel_close(rel, NoLock);
    end_replication_step();
}
```