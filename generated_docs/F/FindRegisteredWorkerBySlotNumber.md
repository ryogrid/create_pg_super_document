# FindRegisteredWorkerBySlotNumber

## Location
[src/backend/postmaster/bgworker.c:221-245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L221-L245)

## Overview
Searches the postmaster's private list of background workers to find the RegisteredBgWorker object that corresponds to a given shared memory slot number.

## Definition


## Detailed Description
This static function performs a linear search through the postmaster's backend-private BackgroundWorkerList to locate the RegisteredBgWorker structure that maps to the specified shared memory slot number. Each registered worker maintains a correspondence between its entry in the private list and its assigned slot in shared memory through the rw_shmem_slot field. This mapping enables the postmaster to efficiently look up worker metadata when processing shared memory slot-based events or state changes.

## Parameters / Member Variables
- : The shared memory slot number to search for (integer index into the shared memory worker array)

## Dependencies
- Functions called/Symbols referenced:
  -  (single-linked list iterator type)
  -  (macro for iterating through the list)
  -  (macro to extract container from list node)
  -  (worker registration structure type)
  - Global variable:  (postmaster's private worker list)

- Called from (representative examples):
  -  (src/backend/postmaster/bgworker.c:285)

## Notes and Other Information
- Static function, only accessible within the bgworker.c module
- Returns NULL if no worker is found with the specified slot number
- Performs linear search, but typically efficient due to small number of background workers
- Critical for maintaining the correspondence between private worker list and shared memory slots
- Used primarily for handling worker state changes and lifecycle management
- The slot number serves as the key for mapping between the postmaster's private data and shared memory