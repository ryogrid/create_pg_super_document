# ParallelSlotsAdoptConn

## Location
src/fe_utils/parallel_slot.c: 460 - 478

## Overview
Assigns an open database connection to a parallel slots array for reuse, transferring ownership from the caller to the slots management system.

## Definition


## Detailed Description
ParallelSlotsAdoptConn takes ownership of an already established database connection and assigns it to an available slot in the parallel slots array for subsequent reuse. This function is part of PostgreSQL's parallel slot management system used by client utilities to efficiently manage multiple database connections for parallel operations.

The function searches for an unconnected slot in the slots array and assigns the provided connection to it. If no slots are available, the connection is immediately closed and discarded. Once adopted, the caller should not use or close the connection as ownership has been transferred to the slots array.

The connection's parameters (user, host, port, etc.) should match those configured for the slots array, except possibly the database name. If parameters differ, the behavior is undefined.

## Parameters / Member Variables
- : Pointer to the ParallelSlotArray structure that will adopt the connection
- : The established PGconn database connection to be adopted by the slots array

## Dependencies
- Functions called/Symbols referenced:
  - find_unconnected_slot
  - disconnectDatabase
  - ParallelSlotArray
- Called from (representative examples):
  - main (src/bin/pg_amcheck/pg_amcheck.c:705)
  - reindex_one_database (src/bin/scripts/reindexdb.c:421)
  - vacuum_one_database (src/bin/scripts/vacuumdb.c:829)
  - ParallelSlotClearHandler (src/include/fe_utils/parallel_slot.h:68)

## Notes and Other Information
- This function is part of the frontend utilities parallel slot management system
- The caller must not use or close the connection after calling this function
- Connection parameters should match the slots array configuration for proper operation
- If no available slots exist, the connection is automatically closed rather than stored
- Located in src/fe_utils/parallel_slot.c:460-478