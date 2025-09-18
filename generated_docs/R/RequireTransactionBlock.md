# RequireTransactionBlock

## Location
[src/backend/access/transam/xact.c:3662-3670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3662-L3670)

## Overview
RequireTransactionBlock enforces that certain commands must execute within transaction blocks by raising errors when they're executed outside of them, ensuring proper transaction semantics for commands that require transactional context.

## Definition


## Detailed Description
This function is a simple wrapper around CheckTransactionBlock that enforces strict transaction block requirements for commands that must execute within a transactional context. Unlike WarnNoTransactionBlock which issues warnings, this function raises errors when commands are executed outside transaction blocks.

The function serves as the strict counterpart to WarnNoTransactionBlock - it's used for commands where execution outside a transaction block represents a genuine error condition rather than just poor practice. It's designed for commands like DECLARE CURSOR that have no meaningful effect outside of a transaction context.

## Parameters / Member Variables
- : bool - indicates whether the statement is being executed at the top level (not inside a function)
- : const char* - name of the statement type for error message formatting

## Dependencies
- Functions called/Symbols referenced:
  - [CheckTransactionBlock](../C/CheckTransactionBlock.md) (with isTopLevel, true, stmtType parameters)
- Called from:
  - [PerformCursorOpen](../P/PerformCursorOpen.md) (for DECLARE CURSOR statements)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (for various statement types that require transaction contexts)

## Notes and Other Information
This function implements the 'strict' approach to transaction block validation, using errors instead of warnings when commands are executed outside transaction blocks. It's typically used for commands like DECLARE CURSOR that are fundamentally meaningless outside of a transaction context. The boolean 'true' parameter passed to CheckTransactionBlock distinguishes this from the warning-only behavior of WarnNoTransactionBlock.