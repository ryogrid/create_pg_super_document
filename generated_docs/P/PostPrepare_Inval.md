# PostPrepare_Inval

## Location
src/backend/utils/cache/inval.c: 864 - 882

## Overview
Cleans up invalidation state after successful PREPARE by undoing syscache changes to maintain consistency with the external world view.

## Definition


## Detailed Description
PostPrepare_Inval is called after a successful PREPARE statement in two-phase commit transactions. When a transaction is prepared, it exists in a limbo state where the transaction has been durably recorded but is not yet visible to other transactions. During this state, the system must maintain cache consistency by acting as if the transaction had aborted rather than committed.

The function calls AtEOXact_Inval(false), which processes invalidation messages as if the transaction were aborting. This ensures that:
1. Any syscache changes made by the prepared transaction are undone locally
2. The backend's cache state remains synchronized with the external world view
3. No invalidation messages are sent to other backends (since they haven't seen the changes)

If the prepared transaction is later committed via COMMIT PREPARED, the system will receive and process the appropriate invalidation messages normally. If it's rolled back via ROLLBACK PREPARED, no additional invalidation work is needed.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [AtEOXact_Inval](../A/AtEOXact_Inval.md)
- Called from (representative examples):
  - [PrepareTransaction](PrepareTransaction.md)

## Notes and Other Information
- Essential for maintaining cache consistency during two-phase commit operations
- The function ensures that prepared transactions don't leave stale cache entries
- Acts as if the transaction aborted to maintain consistency with external transaction state
- Critical for proper operation of distributed transactions and two-phase commit protocols
- The prepared transaction's changes become visible only after COMMIT PREPARED, at which point normal invalidation messages are processed
- Ensures ACID properties are maintained during the prepared state of two-phase transactions