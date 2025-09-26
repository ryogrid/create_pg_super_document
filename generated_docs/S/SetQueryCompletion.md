# SetQueryCompletion

## Location
[src/include/tcop/cmdtag.h:37-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tcop/cmdtag.h#L37-L44)

## Overview
A static inline function that initializes a QueryCompletion structure with a command tag and the number of rows processed.

## Definition
```c
static inline void SetQueryCompletion(QueryCompletion *qc, CommandTag commandTag, uint64 nprocessed)
```

## Detailed Description
SetQueryCompletion is a utility function that populates a QueryCompletion structure with the results of a completed SQL command. The function sets both the command type identifier and the count of rows affected by the command. This is commonly used throughout PostgreSQL to standardize how query completion information is recorded and reported back to clients.

The function is defined as a static inline function in the header file, making it available for efficient inline expansion at compile time wherever it is used.

## Parameters / Member Variables
- `qc`: Pointer to the QueryCompletion structure to be populated
- `commandTag`: The CommandTag enum value identifying the type of SQL command that was executed
- `nprocessed`: The number of rows that were processed/affected by the command

## Dependencies
- Functions called/Symbols referenced:
  - QueryCompletion (structure type)
  - CommandTag (enum type)
- Called from (representative examples):
  - ExecCreateTableAs
  - RefreshMatViewByOid
  - PerformPortalFetch
  - ProcessQuery
  - standard_ProcessUtility
  - WalSndDone
  - StartLogicalReplication

## Notes and Other Information
- This is a static inline function defined in src/include/tcop/cmdtag.h
- The function is used extensively throughout the codebase for standardizing query completion reporting
- The QueryCompletion structure it populates is used to communicate command results back to clients
- The commandTag parameter comes from an enum that includes all supported SQL command types
- The nprocessed count is particularly important for commands like INSERT, UPDATE, DELETE that affect multiple rows