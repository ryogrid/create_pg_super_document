# SharedRecordTypmodRegistryEstimate

## Location
[src/backend/utils/cache/typcache.c:2086-2107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2086-L2107)

## Overview
Returns the memory size required to hold a SharedRecordTypmodRegistry structure in shared memory, providing a clean interface without exposing internal structure details in headers.

## Definition


## Detailed Description
This function serves as a memory size estimation utility for SharedRecordTypmodRegistry structures in PostgreSQL's shared memory management. It encapsulates the size calculation logic to maintain clean separation between interface and implementation. The function is specifically designed to support shared memory allocation planning for parallel query operations that need to exchange non-anonymous record types between backends.

The function returns the exact size needed for a SharedRecordTypmodRegistry, which contains hash table handles for record type lookup and typmod assignment, along with an atomic counter for generating new typmod numbers.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [SharedRecordTypmodRegistry](SharedRecordTypmodRegistry.md) (struct type)
- Called from (representative examples):
  - [GetSessionDsmHandle](../G/GetSessionDsmHandle.md)

## Notes and Other Information
- This function exists specifically to avoid exposing the private internal structure of SharedRecordTypmodRegistry in header files
- Used in shared memory size estimation for parallel query coordination
- The SharedRecordTypmodRegistry structure contains dshash table handles and atomic counters for managing record types across parallel backends
- Part of PostgreSQL's type cache system for handling composite/record types in parallel execution contexts