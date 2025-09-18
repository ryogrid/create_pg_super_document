# MarkSubxactTopXidLogged

## Location
src/backend/access/transam/xact.c: 588 - 603

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
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - IsSubxactTopXidLogPending (in assertion)
- Called from (representative examples):
  - XLogInsertRecord

## Notes and Other Information
- Must only be called when IsSubxactTopXidLogPending() returns true
- Sets the topXidLogged flag to prevent redundant logging
- Part of PostgreSQL's logical decoding support for subtransactions
- Works in conjunction with IsSubxactTopXidLogPending() to manage logging state
- Essential for maintaining proper transaction hierarchy information in WAL
- Located in src/backend/access/transam/xact.c:588-603