# TransactionIdInRecentPast

## Location
[src/backend/utils/adt/xid8funcs.c:97-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L97-L152)

## Overview
Validates that a given transaction ID and epoch are within reasonable bounds, checking that they are not in the future and not so far back that they have wrapped around.

## Definition

```c
static bool
TransactionIdInRecentPast(FullTransactionId fxid, TransactionId *extracted_xid)
```
## Detailed Description
This function performs sanity checks on transaction ID (xid) and epoch pairs to ensure they represent valid, recent transaction identifiers. It validates that the provided xid/epoch combination is not in the future relative to the current system state and not so far in the past that the transaction ID counter has wrapped around multiple times.

The function works by comparing the provided epoch with the next transaction's epoch. The epoch should either match the current epoch (if xid <= nextXid) or be one less than the current epoch (if xid > nextXid, indicating the counter wrapped). This validation is crucial for replication scenarios where standby servers send feedback about their transaction processing state.

## Parameters / Member Variables
- : The transaction ID to validate
- : The epoch associated with the transaction ID

## Dependencies
- Functions called/Symbols referenced:
  - ReadNextFullTransactionId
  - XidFromFullTransactionId
  - EpochFromFullTransactionId
  - [TransactionIdPrecedesOrEquals](TransactionIdPrecedesOrEquals.md)
- Called from (representative examples):
  - [ProcessStandbyHSFeedbackMessage](../P/ProcessStandbyHSFeedbackMessage.md) (in walsender.c)
  - [pg_xact_status](../p/pg_xact_status.md) (in xid8funcs.c)

## Notes and Other Information
- This is a static function used internally within the WAL sender subsystem
- The function specifically notes that it doesn't care about whether clog (commit log) exists for the transaction IDs
- Critical for maintaining consistency in master-standby replication scenarios
- Helps prevent issues with transaction ID wraparound in distributed PostgreSQL setups