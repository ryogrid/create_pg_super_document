# PrepareTempTablespaces

## Location
src/backend/commands/tablespace.c: 1331 - 1425

## Overview
Parses the temp_tablespaces GUC variable and configures the storage subsystem to use the specified tablespaces for temporary files, performing validation and permission checks.

## Definition


## Detailed Description
This function ensures that temporary file storage is properly configured according to the temp_tablespaces GUC setting. It is designed to be called multiple times safely - it performs no work if tablespaces have already been set up in the current transaction.

The function performs several steps:
1. **Early exit checks**: Returns immediately if tablespaces are already configured or if not in a transaction
2. **String parsing**: Parses the temp_tablespaces GUC value as a comma-separated list of tablespace names
3. **Validation loop**: For each tablespace name, verifies existence and checks CREATE permissions
4. **Memory allocation**: Stores valid tablespace OIDs in TopTransactionContext for transaction-duration persistence
5. **Configuration**: Calls SetTempTablespaces to inform the storage system of available tablespaces

Special handling includes allowing empty strings and explicit database default tablespace references, both of which are converted to InvalidOid. Invalid or inaccessible tablespaces are silently skipped rather than causing errors.

## Parameters
None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - TempTablespacesAreSet
  - IsTransactionState
  - SplitIdentifierString
  - get_tablespace_oid
  - object_aclcheck
  - SetTempTablespaces
  - MemoryContextAlloc
  - list_free
- Called from (representative examples):
  - GetDefaultTablespace (src/backend/commands/tablespace.c:1150)
  - ExecHashTableCreate (src/backend/executor/nodeHash.c:586)
  - BufFileCreateTemp (src/backend/storage/file/buffile.c:207)
  - inittapestate (src/backend/utils/sort/tuplesort.c:1965)

## Notes and Other Information
- Called once per transaction, subsequent calls in the same transaction are no-ops
- Silently skips invalid tablespace names rather than throwing errors
- Uses TopTransactionContext for memory allocation to ensure persistence across the transaction
- Falls back gracefully when called outside a transaction (storage system uses database default)
- Converts explicit database default tablespace references to InvalidOid for consistency
- Essential for operations that create temporary files like sorting, hashing, and tuple storage