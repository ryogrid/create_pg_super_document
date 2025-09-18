# AtEOXact_SMgr

## Location
[src/backend/storage/smgr/smgr.c:833-842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L833-L842)

## Overview
Called during transaction commit or abort to destroy all unpinned SMgrRelation objects as part of resource cleanup and file descriptor management.

## Definition
void AtEOXact_SMgr(void)

## Detailed Description
The AtEOXact_SMgr function serves as a transaction cleanup handler in PostgreSQL's storage manager subsystem. It is called at the end of both successful and failed transactions (commit or abort) to perform essential resource management. The function destroys all unpinned SMgrRelation objects by calling smgrdestroyall().

This cleanup strategy represents a compromise in resource management: transient SMgrRelation objects are allowed to live for some time to amortize the costs of operations like blind writes of multiple blocks, but they cannot live forever. This is crucial because these objects typically hold open kernel file descriptors for underlying files, and these descriptors must be closed reasonably soon, especially if the associated files are deleted.

The function is part of PostgreSQL's "AtEOXact" (At End Of Transaction) callback system, ensuring proper cleanup regardless of transaction outcome.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [smgrdestroyall](../s/smgrdestroyall.md) (destroys all unpinned storage manager relations)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md) (in xact.c at line 2416)
  - [PrepareTransaction](../P/PrepareTransaction.md) (in xact.c at line 2705)
  - [AbortTransaction](AbortTransaction.md) (in xact.c at line 2925)
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md) (in bgwriter.c at line 174)
  - [CheckpointerMain](../C/CheckpointerMain.md) (in checkpointer.c at line 277)
  - [WalWriterMain](../W/WalWriterMain.md) (in walwriter.c at line 172)

## Notes and Other Information
- Called for both transaction commit and abort - the function doesn't distinguish between the two cases
- Part of PostgreSQL's resource management strategy to balance performance and resource usage
- Ensures kernel file descriptors are not leaked, particularly important when files are deleted
- Only destroys unpinned SMgrRelation objects, preserving those that are still actively referenced
- Critical for proper cleanup in both foreground transactions and background processes
- Helps prevent file descriptor exhaustion in long-running PostgreSQL instances