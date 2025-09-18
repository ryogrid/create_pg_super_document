# dupEvents

## Location
[src/interfaces/libpq/fe-exec.c:408-451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L408-L451)

## Overview
Creates a deep copy of an array of PGEvent structures, duplicating event names but resetting instance data and initialization flags for the new copy.

## Definition


## Detailed Description
dupEvents performs a specialized duplication of PGEvent arrays used in libpq's event system. The function allocates memory for a new event array and copies the essential event properties (procedure pointer, passThrough data, and name) while deliberately resetting instance-specific data. The data field is set to NULL and resultInitialized is set to false, ensuring that copied events start in a clean state.

The function carefully manages memory allocation, including tracking the total allocated size in the memSize parameter. If any allocation fails during the copying process, it performs complete cleanup by freeing all previously allocated names and the event array itself.

## Parameters / Member Variables
- : Source array of PGEvent structures to duplicate
- : Number of events in the source array
- : Pointer to size accumulator that tracks total allocated memory

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - strdup
  - free
  - strlen
- Called from (representative examples):
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md)
  - [PQcopyResult](../P/PQcopyResult.md)

## Notes and Other Information
- Returns NULL if source events is NULL, count is zero, or memory allocation fails
- Performs complete cleanup on failure, ensuring no memory leaks
- Deliberately resets data and resultInitialized fields for new event instances
- Updates the memSize parameter to include all allocated memory (array + strings)
- Static function, internal to fe-exec.c module
- Essential for proper event system functionality when copying connections and results