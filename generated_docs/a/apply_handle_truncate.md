# apply_handle_truncate

## Location
[src/backend/replication/logical/worker.c:3157-3284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L3157-L3284)

## Overview
Handles TRUNCATE message processing in PostgreSQL's logical replication worker, implementing table truncation for subscribed relations including partitioned tables.

## Definition

```c
static void
apply_handle_truncate(StringInfo s)
```
## Detailed Description
This function processes TRUNCATE messages received from the publisher in logical replication. It handles the complete truncation workflow including:

1. **Message Parsing**: Reads the truncate message to extract target relations, cascade behavior, and sequence restart options
2. **Relation Validation**: Opens and validates each target relation, ensuring the subscriber has appropriate permissions
3. **Partitioned Table Handling**: For partitioned tables, automatically includes all partitions in the truncation operation using find_all_inheritors
4. **Permission Checking**: Validates ACL_TRUNCATE privileges on all target relations and their partitions
5. **Execution**: Delegates to ExecuteTruncateGuts to perform the actual truncation with appropriate options
6. **Resource Cleanup**: Closes all opened relations and releases locks

The function includes comprehensive handling of edge cases such as temporary tables from other backends and ensures that partition hierarchies are properly truncated. It also respects the subscription's runasowner setting to determine execution context.

## Parameters / Member Variables
- `s`: StringInfo containing the serialized TRUNCATE message from the publisher, including relation OIDs, cascade flag, and restart sequences flag
## Dependencies
- Functions called/Symbols referenced:
  - is_skipping_changes
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)  
  - [begin_replication_step](../b/begin_replication_step.md)
  - [logicalrep_read_truncate](../l/logicalrep_read_truncate.md)
  - [logicalrep_rel_open](../l/logicalrep_rel_open.md)
  - [should_apply_changes_for_rel](../s/should_apply_changes_for_rel.md)
  - [TargetPrivilegesCheck](../T/TargetPrivilegesCheck.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - RelationIsLogicallyLogged
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)
  - [end_replication_step](../e/end_replication_step.md)
- Called from (representative examples):
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- Currently marked as TODO for FDW (Foreign Data Wrapper) support, indicating future enhancement plans
- Uses AccessExclusiveLock for relation locking to ensure exclusive access during truncation
- Explicitly uses DROP_RESTRICT behavior regardless of upstream cascade settings for safety
- Handles partitioned tables by automatically discovering and truncating all child partitions
- Includes special handling for temporary tables of other backends (similar to ExecuteTruncate)
- The runasowner subscription setting controls whether operations execute as subscription owner or table owner
- Maintains logged relation lists separately for proper WAL logging of the truncation operation

## Simplified Source

```c
static void
apply_handle_truncate(StringInfo s)
{
    bool cascade = false;
    bool restart_seqs = false;
    List *remote_relids = NIL;
    List *remote_rels = NIL;
    List *rels = NIL;
    List *part_rels = NIL;
    List *relids = NIL;
    List *relids_logged = NIL;
    ListCell *lc;
    LOCKMODE lockmode = AccessExclusiveLock;

    // Skip if not applying changes or handling streamed transactions
    if (is_skipping_changes() ||
        handle_streamed_transaction(LOGICAL_REP_MSG_TRUNCATE, s))
        return;

    begin_replication_step();

    // Parse truncate message to get relation OIDs and options
    remote_relids = logicalrep_read_truncate(s, &cascade, &restart_seqs);

    // Process each target relation
    foreach(lc, remote_relids)
    {
        LogicalRepRelId relid = lfirst_oid(lc);
        LogicalRepRelMapEntry *rel;

        // Open relation and check if we should apply changes
        rel = logicalrep_rel_open(relid, lockmode);
        if (!should_apply_changes_for_rel(rel))
        {
            logicalrep_rel_close(rel, lockmode);
            continue;
        }

        // Add to processing lists
        remote_rels = lappend(remote_rels, rel);
        TargetPrivilegesCheck(rel->localrel, ACL_TRUNCATE);
        rels = lappend(rels, rel->localrel);
        relids = lappend_oid(relids, rel->localreloid);
        if (RelationIsLogicallyLogged(rel->localrel))
            relids_logged = lappend_oid(relids_logged, rel->localreloid);

        // Handle partitioned tables - include all partitions
        if (rel->localrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        {
            List *children = find_all_inheritors(rel->localreloid, lockmode, NULL);
            ListCell *child;

            foreach(child, children)
            {
                Oid childrelid = lfirst_oid(child);
                Relation childrel;

                if (list_member_oid(relids, childrelid))
                    continue;

                childrel = table_open(childrelid, NoLock);

                // Skip temp tables from other backends
                if (RELATION_IS_OTHER_TEMP(childrel))
                {
                    table_close(childrel, lockmode);
                    continue;
                }

                // Add partition to truncation list
                TargetPrivilegesCheck(childrel, ACL_TRUNCATE);
                rels = lappend(rels, childrel);
                part_rels = lappend(part_rels, childrel);
                relids = lappend_oid(relids, childrelid);
                if (RelationIsLogicallyLogged(childrel))
                    relids_logged = lappend_oid(relids_logged, childrelid);
            }
        }
    }

    // Execute the truncation operation
    // Use DROP_RESTRICT for safety regardless of upstream cascade setting
    ExecuteTruncateGuts(rels, relids, relids_logged, DROP_RESTRICT,
                       restart_seqs, !MySubscription->runasowner);

    // Clean up - close all opened relations
    foreach(lc, remote_rels)
    {
        LogicalRepRelMapEntry *rel = lfirst(lc);
        logicalrep_rel_close(rel, NoLock);
    }
    foreach(lc, part_rels)
    {
        Relation rel = lfirst(lc);
        table_close(rel, NoLock);
    }

    end_replication_step();
}
```