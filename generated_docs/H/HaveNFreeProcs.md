# HaveNFreeProcs

## Location
[src/backend/storage/lmgr/proc.c:692-717](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L692-L717)

## Overview
Checks whether there are at least N free PGPROC objects available in the system, providing both a boolean result and the actual count of free processes.

## Definition
bool HaveNFreeProcs(int n, int *nfree)

## Detailed Description
This function determines if there are at least N free PGPROC objects available for allocation to new backend processes. It iterates through the freeProcs list in ProcGlobal to count available PGPROC entries.

The function is optimized for small values of N, as it stops counting once it reaches the requested number rather than counting all free processes. This makes it efficient for typical use cases where only a small number of free processes need to be verified.

The function uses ProcStructLock to ensure thread-safe access to the free process list, preventing race conditions during the counting operation.

## Parameters / Member Variables
- : The minimum number of free PGPROC objects to check for (must be greater than 0)
- : Output parameter that receives either the actual number of free PGPROC objects (if less than n) or n (if at least n are available)

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_iter](../d/dlist_iter.md) (iterator type)
  - dlist_foreach (macro for iterating)

- Called from (representative examples):
  - [InitPostgres](../I/InitPostgres.md)

## Notes and Other Information
- Designed with the assumption that N will generally be small for optimal performance
- Returns true if at least N free PGPROC objects are available, false otherwise
- The nfree parameter is always set regardless of the return value
- Uses ProcStructLock for thread-safe access to the free process list
- The function stops counting early once the requested number is reached for efficiency
- This is typically used during backend initialization to ensure sufficient resources are available
- The function includes assertions to validate that n > 0 and nfree is not NULL