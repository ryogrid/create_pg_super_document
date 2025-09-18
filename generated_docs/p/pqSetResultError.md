# pqSetResultError

## Location
src/interfaces/libpq/fe-exec.c: 692 - 720

## Overview
pqSetResultError assigns a new error message to a PGresult object, handling out-of-memory scenarios gracefully with fallback error messages.

## Definition
```c
void pqSetResultError(PGresult *res, PQExpBuffer errorMessage, int offset)
```

## Detailed Description
pqSetResultError copies an error message from a PQExpBuffer into a PGresult's error message field, starting from a specified offset. The function handles two potential out-of-memory scenarios: when the errorMessage buffer itself is marked as "broken" due to previous allocation failures, or when pqResultStrdup fails to allocate space for the message copy. In either case, it falls back to a constant "out of memory" string to ensure the result always has a valid error message.

## Parameters / Member Variables
- `res`: Pointer to the PGresult structure to receive the error message
- `errorMessage`: PQExpBuffer containing the source error message
- `offset`: Starting position within the errorMessage buffer to copy from

## Dependencies
- Functions called/Symbols referenced:
  - PQExpBufferBroken
  - [pqResultStrdup](pqResultStrdup.md)
  - [libpq_gettext](../l/libpq_gettext.md)
- Called from (representative examples):
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md)
  - [pqPrepareAsyncResult](pqPrepareAsyncResult.md)
  - [pqGetErrorNotice3](pqGetErrorNotice3.md)

## Notes and Other Information
- Provides robust error handling with fallback to constant string for OOM scenarios
- The offset parameter allows copying from specific positions in multi-part error messages
- Uses internationalization via libpq_gettext for the fallback "out of memory" message
- Ensures res->errMsg always points to a valid string, never NULL
- Does nothing if the input PGresult pointer is NULL
- Located at src/interfaces/libpq/fe-exec.c:692-720