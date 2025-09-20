# injection_init_shmem

## Location
[src/test/modules/injection_points/injection_points.c:117-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L117-L136)

## Overview
A static function that initializes the shared memory area for the injection points testing module in PostgreSQL.

## Definition

```c
static void
injection_init_shmem(void)
```
## Detailed Description
This function sets up the shared memory segment for injection points functionality. It uses PostgreSQL's Dynamic Shared Memory (DSM) mechanism to create or attach to a named shared memory segment called "injection_points". The function ensures that the shared memory is only initialized once by checking if the global  pointer is already set. If the segment doesn't exist, it creates one with the specified size and initialization callback; if it already exists, it simply attaches to the existing segment.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL DSM function to get/create named shared memory segment)
  -  (struct type defining the shared memory layout)
  -  (callback function for initializing the shared memory)
- Called from:
  -  (at src/test/modules/injection_points/injection_points.c:209)
  -  (at src/test/modules/injection_points/injection_points.c:330)
  -  (at src/test/modules/injection_points/injection_points.c:370)

## Notes and Other Information
- This is a static function, only accessible within injection_points.c
- Uses lazy initialization pattern - only initializes shared memory when first accessed
- The  variable indicates whether the DSM segment already existed or was newly created
- The shared memory segment is named "injection_points" and sized according to 
- Part of PostgreSQL's testing infrastructure for simulating various runtime conditions and race scenarios