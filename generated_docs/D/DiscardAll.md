# DiscardAll

## Location
src/backend/commands/discard.c: 57 - 78

## Overview
Implements comprehensive session state cleanup for the SQL DISCARD ALL command, resetting all session-level state including portals, prepared statements, locks, and caches.

## Definition
```c
static void DiscardAll(bool isTopLevel)
```

## Detailed Description
DiscardAll performs a comprehensive cleanup of all session state when executing DISCARD ALL. This function ensures that the session returns to a clean state similar to a fresh connection. It systematically cleans up various types of session state including:
- Portal (cursor) cleanup via PortalHashTableDeleteAll()
- Session authorization reset to default
- All configuration options reset to defaults
- All prepared statements dropped
- All LISTEN subscriptions removed
- All user-acquired advisory locks released
- Plan cache cleared
- Temporary table namespace reset
- Sequence caches cleared

The function includes transaction safety checks to prevent DISCARD ALL from being executed within a transaction block, as this would leave the transaction uncommitted and potentially cause confusion.

## Parameters / Member Variables
- `isTopLevel`: Boolean indicating whether this command is executed at the top level, used by PreventInTransactionBlock() to determine if the command is safe to execute outside a transaction

## Dependencies
- Functions called/Symbols referenced:
  - PreventInTransactionBlock
  - PortalHashTableDeleteAll
  - SetPGVariable
  - ResetAllOptions
  - DropAllPreparedStatements
  - Async_UnlistenAll
  - LockReleaseAll
  - ResetPlanCache
  - ResetTempTableNamespace
  - ResetSequenceCaches
  - USER_LOCKMETHOD (constant)
- Called from (representative examples):
  - DiscardCommand

## Notes and Other Information
- The function is declared static, meaning it's only accessible within the discard.c compilation unit
- Portal cleanup is performed first because closing portals might run user-defined code
- The transaction block prevention is a safety measure to catch potential user mistakes
- Session authorization is reset using SetPGVariable with NIL value and false for local-only
- Advisory locks are released using USER_LOCKMETHOD with the allxids parameter set to true
- This function represents one of the most comprehensive session cleanup operations in PostgreSQL
- The order of operations is carefully chosen to handle dependencies between different types of session state