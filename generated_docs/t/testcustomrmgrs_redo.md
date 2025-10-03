# testcustomrmgrs_redo

## Location
[src/test/modules/test_custom_rmgrs/test_custom_rmgrs.c:82-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_custom_rmgrs/test_custom_rmgrs.c#L82-L90)

## Overview
A no-operation redo function for the test custom resource manager that validates WAL record operation codes but performs no actual recovery operations.

## Definition

```c
void
testcustomrmgrs_redo(XLogReaderState *record)
```
## Detailed Description
The  function implements the redo callback for the test_custom_rmgrs custom WAL resource manager. This function is part of the RMGR API and gets called during WAL replay/recovery to apply logged changes.

Since this is a test module that doesn't manage any real data structures requiring recovery, the function is implemented as a no-operation. It only validates that the operation code in the WAL record matches the expected  type. If an unknown operation code is encountered, it triggers a PANIC to indicate corruption or an invalid WAL record.

The function extracts the operation info from the WAL record by masking off the info flags using , leaving only the operation-specific bits for validation.

## Parameters / Member Variables
- `*record`: Pointer to an  structure containing the WAL record being processed during recovery
## Dependencies
- Functions called/Symbols referenced:
  -  (extracts record info/flags)
  -  (constant for masking info flags)
  -  (expected operation code constant)
  -  (error logging with PANIC level)
- Called from (representative examples):
  - PostgreSQL WAL recovery system during startup recovery or streaming replication
  - Custom resource manager framework via  structure

## Notes and Other Information
- This is a test-only implementation designed for validating custom WAL resource manager functionality
- The function purposely does no actual work since it's testing the framework, not real data recovery
- Located in 
- Part of the custom resource manager registered with ID 
- Any unrecognized operation code will cause a system PANIC, ensuring strict validation during testing