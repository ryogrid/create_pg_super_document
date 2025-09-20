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
- : StringInfo containing the serialized TRUNCATE message from the publisher, including relation OIDs, cascade flag, and restart sequences flag

## Dependencies
- Functions called/Symbols referenced:
  - is_skipping_changes
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)  
  - [begin_replication_step](../b/begin_replication_step.md)
  - [logicalrep_read_truncate](../l/logicalrep_read_truncate.md)
  - logicalrep_rel_open
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