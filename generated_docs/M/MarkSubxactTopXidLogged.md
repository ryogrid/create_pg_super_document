# MarkSubxactTopXidLogged

## Location
[src/backend/access/transam/xact.c:588-603](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L588-L603)

## Overview
Marks the top-level transaction ID as having been logged to WAL for the current subtransaction, updating the transaction state to prevent redundant logging.

## Definition
```c
void MarkSubxactTopXidLogged(void)
```

## Detailed Description
This function serves as the state management component for subtransaction top-level XID logging. After the top-level transaction ID has been successfully written to a WAL record during subtransaction operations, this function updates the transaction state to reflect that the logging has been completed.

The function includes an assertion that verifies IsSubxactTopXidLogPending() returns true before proceeding, ensuring that the conditions for top-level XID logging are actually met. Once called, it sets the topXidLogged flag to true, which prevents subsequent operations in the same subtransaction from unnecessarily re-logging the top-level XID.

This is a critical component of PostgreSQL's logical decoding infrastructure, as it ensures the top-level XID is logged exactly once per subtransaction when required for logical replication.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [IsSubxactTopXidLogPending](../I/IsSubxactTopXidLogPending.md) (in assertion)
- Called from (representative examples):
  - [XLogInsertRecord](../X/XLogInsertRecord.md)

## Notes and Other Information
- Must only be called when IsSubxactTopXidLogPending() returns true
- Sets the topXidLogged flag to prevent redundant logging
- Part of PostgreSQL's logical decoding support for subtransactions
- Works in conjunction with IsSubxactTopXidLogPending() to manage logging state
- Essential for maintaining proper transaction hierarchy information in WAL
- Located in src/backend/access/transam/xact.c:588-603

## Simplified Source

```c
// Simplified version of MarkSubxactTopXidLogged
void MarkSubxactTopXidLogged(void) {
    // Verify that top XID logging is actually pending for current subtransaction
    Assert(IsSubxactTopXidLogPending());

    // Mark the top-level transaction ID as now logged to WAL
    CurrentTransactionState->topXidLogged = true;
}
```

Key simplifications made:
- Added descriptive comments explaining the purpose of each operation
- Preserved the essential assertion check and state update logic
- Function is already quite simple with minimal complexity to reduce