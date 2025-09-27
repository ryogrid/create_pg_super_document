# TransactionBlockStatusCode

## Location
[src/backend/access/transam/xact.c:4947-4987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L4947-L4987)

## Overview
TransactionBlockStatusCode returns a single-character status code that indicates the current transaction state to be sent in PostgreSQL's ReadyForQuery message to clients.

## Definition
```c
char TransactionBlockStatusCode(void)
```

## Detailed Description
TransactionBlockStatusCode maps the internal transaction block state to a standardized character code that is sent to clients in the ReadyForQuery message as part of PostgreSQL's frontend/backend protocol. The function examines the current transaction's blockState and returns one of three possible character codes: 'I' for idle (not in transaction), 'T' for active transaction, or 'E' for failed transaction state.

This mapping is essential for client applications to understand the current transaction context and respond appropriately. The function comprehensively covers all possible transaction block states and groups them into the three fundamental categories that clients need to understand for proper transaction management.

## Parameters / Member Variables
This function takes no parameters and returns a character representing the transaction status:
- 'I': Idle (not in transaction) - returned for TBLOCK_DEFAULT and TBLOCK_STARTED states
- 'T': In transaction - returned for active transaction states including BEGIN, INPROGRESS, END, PREPARE, and subtransaction states
- 'E': In failed transaction - returned for all abort-related states

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - TransactionState (type)
  - Multiple TBLOCK_* enum constants (DEFAULT, STARTED, BEGIN, SUBBEGIN, INPROGRESS, etc.)
  - [BlockStateAsString](../B/BlockStateAsString.md) (for error reporting)
- Called from (representative examples):
  - [ReadyForQuery](../R/ReadyForQuery.md)

## Notes and Other Information
The function includes comprehensive error handling with a FATAL error if an invalid transaction block state is encountered, though this should never occur in normal operation. The mapping treats TBLOCK_STARTED as idle ('I') rather than active ('T'), which aligns with the semantic that a started but unused transaction is effectively idle from the client's perspective. This function is critical for the PostgreSQL wire protocol and ensures clients receive accurate transaction state information.

## Simplified Source

```c
// Simplified version of TransactionBlockStatusCode
char TransactionBlockStatusCode(void) {
    TransactionState current_state = CurrentTransactionState;

    // Check current transaction block state and return appropriate status code
    switch (current_state->blockState) {
        // Idle states - not in an active transaction
        case TBLOCK_DEFAULT:
        case TBLOCK_STARTED:
            return 'I';  // Idle - not in transaction

        // Active transaction states - transaction in progress
        case TBLOCK_BEGIN:
        case TBLOCK_SUBBEGIN:
        case TBLOCK_INPROGRESS:
        case TBLOCK_IMPLICIT_INPROGRESS:
        case TBLOCK_PARALLEL_INPROGRESS:
        case TBLOCK_SUBINPROGRESS:
        case TBLOCK_END:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_PREPARE:
            return 'T';  // In transaction

        // Failed transaction states - transaction aborted/failed
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
        case TBLOCK_ABORT_END:
        case TBLOCK_SUBABORT_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
            return 'E';  // In failed transaction
    }

    // Should never reach here - invalid state
    elog(FATAL, "invalid transaction block state: %s",
         BlockStateAsString(current_state->blockState));
    return 0;
}
```

Key simplifications made:
- Added descriptive variable name `current_state` for better readability
- Grouped switch cases logically with comments explaining each category
- Enhanced comments to clarify the meaning of each return value
- Maintained the essential error handling for invalid states
- Preserved the exact logic flow and return values