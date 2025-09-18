# hash_corrupted

## Location
[src/backend/utils/hash/dynahash.c:1740-1753](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1740-L1753)

## Overview
Handles hash table corruption detection by logging appropriate error messages and terminating execution.

## Definition


## Detailed Description
This function serves as the error handler for hash table corruption scenarios. When called, it determines the severity of the response based on whether the corrupted hash table is shared among multiple processes or local to a single backend. For shared hash tables, corruption poses a system-wide risk, so the function triggers a PANIC-level error that forces a complete PostgreSQL cluster restart. For non-shared hash tables, it issues a FATAL error that terminates only the current backend process.

The function provides diagnostic information by including the hash table name in the error message, which helps administrators and developers identify which specific hash table experienced corruption.

## Parameters / Member Variables
- : Pointer to the HTAB structure representing the corrupted hash table

## Dependencies
- Functions called/Symbols referenced:
  - elog (PostgreSQL logging function)
  - PANIC (error level constant for system-wide failures)
  - FATAL (error level constant for backend termination)
- Data structures referenced:
  - [HTAB](../H/HTAB.md) (hash table structure)
- Called from (representative examples):
  - [hash_initial_lookup](hash_initial_lookup.md)

## Notes and Other Information
- Never returns - always terminates the process or entire system
- Uses different error levels (PANIC vs FATAL) based on hash table sharing status
- PANIC level ensures cluster-wide restart for shared table corruption to prevent data inconsistency
- FATAL level terminates only the current backend for local table corruption
- Includes hash table name (tabname) in error message for debugging purposes
- Critical for maintaining PostgreSQL data integrity by preventing continued operation with corrupted hash structures