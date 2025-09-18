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
  - logicalrep_message_type (converts message type enum to string)
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