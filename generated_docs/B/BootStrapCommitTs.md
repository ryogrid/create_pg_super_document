# BootStrapCommitTs

## Location
[src/backend/access/transam/commit_ts.c:596-614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L596-L614)

## Overview
A bootstrap function called once during PostgreSQL system installation to initialize the CommitTS (commit timestamp) subsystem, though it currently performs no operations.

## Definition
```c
void BootStrapCommitTs(void)
```

## Detailed Description
This function is part of PostgreSQL's bootstrap process and is designed to be called exactly once during system installation (typically by initdb). Unlike other SLRU (Simple Least Recently Used) modules that perform significant initialization during bootstrap, the CommitTS module defers most of its setup work.

The function currently contains no active code, as indicated by the comment explaining that segments are created later when the server starts with the commit timestamp module enabled. This deferred initialization approach is handled by the `ActivateCommitTs` function instead.

The design assumes that:
1. The CommitTS directory structure has already been created by initdb
2. `CommitTsShmemInit` has been called to set up shared memory structures
3. Actual segment file creation will happen when needed during normal operation

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - None (function body is empty)
- Called from (representative examples):
  - [BootStrapXLOG](BootStrapXLOG.md)

## Notes and Other Information
- This function must be called exactly ONCE during system installation
- Unlike most other SLRU bootstrap functions, this one performs no immediate initialization
- The actual segment creation is deferred to `ActivateCommitTs` when the commit timestamp feature is first enabled
- This lazy initialization approach reduces startup overhead when commit timestamp tracking is not needed
- The function maintains the standard bootstrap interface for consistency with other PostgreSQL subsystems
- The CommitTS directory structure is assumed to exist (created by initdb) before this function is called