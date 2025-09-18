# GetCurrentTransactionStopTimestamp

## Location
[src/backend/access/transam/xact.c:888-910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L888-L910)

## Overview
Returns the timestamp marking when the current transaction stopped (committed or aborted), lazily setting it if not already established.

## Definition
TimestampTz GetCurrentTransactionStopTimestamp(void)

## Detailed Description
This function returns the timestamp when the current transaction completed (either committed or aborted). The function implements lazy initialization: if the transaction stop timestamp hasn't been set yet (xactStopTimestamp == 0), it captures the current timestamp and stores it. This can happen when PostgreSQL decides not to write an XLOG record for the transaction but still needs the stop timestamp for other purposes like statistics reporting.

The function includes assertions to ensure it's only called after transaction commit/abort processing has begun, when the transaction state is in one of the final states (DEFAULT, COMMIT, ABORT, or PREPARE).

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - TransactionState (type)
  - PG_USED_FOR_ASSERTS_ONLY (macro)
  - TRANS_DEFAULT, TRANS_COMMIT, TRANS_ABORT, TRANS_PREPARE (transaction state constants)
  - [GetCurrentTimestamp](GetCurrentTimestamp.md)
  - xactStopTimestamp (global variable)
- Called from (representative examples):
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md)
  - [pgstat_report_stat](../p/pgstat_report_stat.md)
  - [pgstat_relation_flush_cb](../p/pgstat_relation_flush_cb.md)

## Notes and Other Information
- The function uses lazy initialization to set the stop timestamp only when first needed
- Contains runtime assertions to ensure proper calling context (transaction must be in a final state)
- The stop timestamp can be set implicitly by this function or explicitly during transaction logging
- Used primarily for transaction logging and statistics collection
- The return type TimestampTz includes timezone information
- Critical for maintaining consistent timing information across transaction completion processing