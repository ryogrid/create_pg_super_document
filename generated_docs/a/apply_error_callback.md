# apply_error_callback

## Location
[src/backend/replication/logical/worker.c:4969-5040](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4969-L5040)

## Overview
An error callback function that provides detailed contextual information about logical replication operations when errors occur, including transaction details, relation information, and specific column references.

## Definition
void apply_error_callback(void *arg)

## Detailed Description
This function serves as an error callback handler for PostgreSQL's logical replication system. When errors occur during the processing of replicated changes, this function is called to provide rich contextual information that helps diagnose the problem.

The function accesses a global ApplyErrorCallbackArg structure that contains information about the current replication operation being processed. It formats different types of error context messages depending on what information is available:

1. **General transaction context**: When no specific relation is being processed
2. **Relation-specific context**: When processing changes for a particular table
3. **Column-specific context**: When processing a specific column of a relation

The error messages include various pieces of contextual information such as:
- Replication origin name
- Message type being processed (INSERT, UPDATE, DELETE, etc.)
- Transaction ID from the remote server
- LSN (Log Sequence Number) information
- Target relation (schema.table)
- Specific column name when applicable

This rich error context is crucial for debugging logical replication issues, especially in complex multi-database setups.

## Parameters / Member Variables
- : A void pointer parameter (currently unused, as the function accesses a global variable instead)

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_message_type](../l/logicalrep_message_type.md) (converts message type enum to string)
  - TransactionIdIsValid (checks if transaction ID is valid)
  - XLogRecPtrIsInvalid (checks if LSN is invalid)
  - errcontext (PostgreSQL error context reporting function)
  - LSN_FORMAT_ARGS (macro for formatting LSN values)
- Called from (representative examples):
  - [LogicalParallelApplyLoop](../L/LogicalParallelApplyLoop.md) (in parallel apply worker)
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md) (in main apply worker)

## Notes and Other Information
- The function uses a global variable apply_error_callback_arg of type ApplyErrorCallbackArg
- The callback is registered with PostgreSQL's error handling system to provide context during errors
- Different error message formats are used based on the availability of relation and attribute information
- The function gracefully handles cases where various pieces of context information may not be available
- This is part of PostgreSQL's logical replication worker infrastructure
- Located in src/backend/replication/logical/worker.c:4969-5040

## Simplified Source

```c
void
apply_error_callback(void *arg)
{
    ApplyErrorCallbackArg *errarg = &apply_error_callback_arg;

    // Skip if no command set
    if (apply_error_callback_arg.command == 0)
        return;

    Assert(errarg->origin_name);

    if (errarg->rel == NULL) {
        // General transaction context - no specific relation
        if (!TransactionIdIsValid(errarg->remote_xid))
            errcontext("processing remote data for replication origin \"%s\" during message type \"%s\"",
                       errarg->origin_name, logicalrep_message_type(errarg->command));
        else if (XLogRecPtrIsInvalid(errarg->finish_lsn))
            errcontext("processing remote data for replication origin \"%s\" during message type \"%s\" in transaction %u",
                       errarg->origin_name, logicalrep_message_type(errarg->command), errarg->remote_xid);
        else
            errcontext("processing remote data for replication origin \"%s\" during message type \"%s\" in transaction %u, finished at %X/%X",
                       errarg->origin_name, logicalrep_message_type(errarg->command),
                       errarg->remote_xid, LSN_FORMAT_ARGS(errarg->finish_lsn));
    } else {
        // Relation-specific context
        if (errarg->remote_attnum < 0) {
            // Table-level operations
            if (XLogRecPtrIsInvalid(errarg->finish_lsn))
                errcontext("processing remote data for replication origin \"%s\" during message type \"%s\" for replication target relation \"%s.%s\" in transaction %u",
                           errarg->origin_name, logicalrep_message_type(errarg->command),
                           errarg->rel->remoterel.nspname, errarg->rel->remoterel.relname, errarg->remote_xid);
            else
                errcontext("processing remote data for replication origin \"%s\" during message type \"%s\" for replication target relation \"%s.%s\" in transaction %u, finished at %X/%X",
                           errarg->origin_name, logicalrep_message_type(errarg->command),
                           errarg->rel->remoterel.nspname, errarg->rel->remoterel.relname,
                           errarg->remote_xid, LSN_FORMAT_ARGS(errarg->finish_lsn));
        } else {
            // Column-specific operations
            if (XLogRecPtrIsInvalid(errarg->finish_lsn))
                errcontext("processing remote data for replication origin \"%s\" during message type \"%s\" for replication target relation \"%s.%s\" column \"%s\" in transaction %u",
                           errarg->origin_name, logicalrep_message_type(errarg->command),
                           errarg->rel->remoterel.nspname, errarg->rel->remoterel.relname,
                           errarg->rel->remoterel.attnames[errarg->remote_attnum], errarg->remote_xid);
            else
                errcontext("processing remote data for replication origin \"%s\" during message type \"%s\" for replication target relation \"%s.%s\" column \"%s\" in transaction %u, finished at %X/%X",
                           errarg->origin_name, logicalrep_message_type(errarg->command),
                           errarg->rel->remoterel.nspname, errarg->rel->remoterel.relname,
                           errarg->rel->remoterel.attnames[errarg->remote_attnum],
                           errarg->remote_xid, LSN_FORMAT_ARGS(errarg->finish_lsn));
        }
    }
}
```