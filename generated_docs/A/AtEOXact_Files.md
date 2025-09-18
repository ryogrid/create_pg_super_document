# AtEOXact_Files

## Location
src/backend/storage/file/fd.c: 3162 - 3175

## Overview
Handles cleanup of file descriptors and temporary files at the end of a transaction, closing open temporary files and resetting temporary tablespace configuration.

## Definition
```c
void AtEOXact_Files(bool isCommit)
```

## Detailed Description
This function is called at transaction end (either commit or abort) to perform comprehensive cleanup of file-related resources. It serves as a high-level coordinator that:

1. Calls CleanupTempFiles to close all open per-transaction temporary file VFDs and delete underlying temporary files
2. Resets the transaction-local temporary tablespace configuration by clearing tempTableSpaces and setting numTempTableSpaces to -1

The function ensures that no temporary files or file descriptors leak beyond transaction boundaries, maintaining proper resource management in PostgreSQL's transactional system.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether the transaction is being committed (true) or aborted (false). This flag is passed to CleanupTempFiles to control warning emission about unclosed files.

## Dependencies
- Functions called/Symbols referenced:
  - CleanupTempFiles (performs the actual cleanup of temporary files)
  - tempTableSpaces (global variable reset to NULL)
  - numTempTableSpaces (global variable reset to -1)
- Called from (representative examples):
  - CommitTransaction (in src/backend/access/transam/xact.c:2417)
  - AbortTransaction (in src/backend/access/transam/xact.c:2926)
  - PrepareTransaction (in src/backend/access/transam/xact.c:2706)
  - Various background processes (bgwriter, checkpointer, walwriter, etc.)

## Notes and Other Information
- This function is part of PostgreSQL's transactional resource management system
- It's called by both foreground transactions and background processes to ensure clean resource state
- The temporary tablespace reset ensures that transaction-local tablespace settings don't persist beyond transaction boundaries
- Works in conjunction with ResourceOwner cleanup mechanisms for comprehensive resource management