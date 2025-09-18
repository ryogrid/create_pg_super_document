# PreventCommandIfReadOnly

## Location
[src/backend/tcop/utility.c:404-421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L404-L421)

## Overview
PreventCommandIfReadOnly throws an error if the current transaction is read-only, providing consistent error messaging for commands that cannot execute in read-only transactions.

## Definition
`void PreventCommandIfReadOnly(const char *cmdname)`

## Detailed Description
This function serves as a centralized check and error reporting mechanism for commands that are incompatible with read-only transactions. It examines the XactReadOnly global variable and raises a standardized error if the current transaction is marked as read-only.

The function ensures consistency in error message wording across the codebase, as some callers may have already checked XactReadOnly themselves but still need to generate the appropriate error message. The error includes the command name in a user-friendly format to help users understand which specific command was rejected.

## Parameters / Member Variables
- `cmdname`: String containing the name of the SQL command being executed (e.g., "CREATE", "INSERT") for inclusion in the error message

## Dependencies
- Functions called/Symbols referenced:
  - XactReadOnly (global variable indicating read-only transaction state)
  - ereport (error reporting mechanism)
  - [errcode](../e/errcode.md), errmsg (error handling macros)
- Called from (representative examples):
  - [DoCopy](../D/DoCopy.md) (src/backend/commands/copy.c:301)
  - [nextval_internal](../n/nextval_internal.md) (src/backend/commands/sequence.c:659)
  - [ExecCheckXactReadOnly](../E/ExecCheckXactReadOnly.md) (src/backend/executor/execMain.c:810)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (src/backend/tcop/utility.c:578)

## Notes and Other Information
- Uses ERRCODE_READ_ONLY_SQL_TRANSACTION error code for consistent error classification
- Commonly used in sequence operations, large object functions, and utility command processing
- Part of PostgreSQL's transaction isolation and read-only enforcement framework
- Provides translator-friendly error messages with command name substitution