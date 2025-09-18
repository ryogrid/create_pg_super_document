# pqSaveErrorResult

## Location
src/interfaces/libpq/fe-exec.c: 803 - 819

## Overview
pqSaveErrorResult is an internal libpq function that marks a connection as having an error condition when returning a failure code is impractical, ensuring error state is properly tracked for later reporting.

## Definition
```c
void pqSaveErrorResult(PGconn *conn)
```

## Detailed Description
pqSaveErrorResult provides a mechanism for recording error conditions in situations where immediate error reporting is not feasible. The function is designed to handle scenarios where:

1. **Immediate failure reporting is impractical**: When the calling context doesn't allow for returning failure codes
2. **Out-of-memory conditions**: When memory allocation for error messages or PGresult structures might fail
3. **Deferred error handling**: When errors need to be reported later in the processing pipeline

The function performs two key operations:
1. **State cleanup**: Calls pqClearAsyncResult() to clear any pending results
2. **Error marking**: Sets the error_result flag to true, signaling that an error result should be generated later

This design allows libpq to maintain error information even under memory pressure, where creating new PGresult structures might fail.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure where the error condition should be recorded

## Dependencies
- Functions called/Symbols referenced:
  - pqClearAsyncResult (to clear pending results before marking error state)

- Called from (representative examples):
  - pqSaveWriteError (for handling write errors)
  - PQgetResult (during result retrieval)
  - getCopyResult (during COPY operations)
  - pqPipelineProcessQueue (during pipeline processing)
  - pqParseInput3 (during protocol message parsing)
  - handleSyncLoss (during synchronization loss recovery)
  - getRowDescriptions (during row description processing)
  - getParamDescriptions (during parameter description processing)
  - getAnotherTuple (during tuple fetching)
  - pqFunctionCall3 (during function call processing)

## Notes and Other Information
- **Internal function**: This is an internal libpq function, not part of the public API
- **Memory safety**: Designed to work reliably even under out-of-memory conditions
- **Deferred processing**: The actual error result creation happens later, typically in PQgetResult()
- **State management**: Ensures clean state by clearing pending results before marking error condition
- **Error propagation**: Provides a robust mechanism for error propagation in complex asynchronous scenarios
- **Protocol handling**: Extensively used in protocol processing where immediate error reporting might disrupt message parsing
- **Thread safety**: Used in conjunction with thread locking mechanisms (pgunlock_thread)