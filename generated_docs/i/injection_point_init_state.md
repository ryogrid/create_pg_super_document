# injection_point_init_state

## Location
[src/test/modules/injection_points/injection_points.c:103-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L103-L116)

## Overview
A static callback function that initializes the shared memory state for injection points in PostgreSQL's testing framework.

## Definition

```c
static void
injection_point_init_state(void *ptr)
```
## Detailed Description
This function serves as a callback for shared memory area initialization in the injection points testing module. It takes a generic pointer to shared memory and casts it to the appropriate  structure, then initializes all the necessary synchronization primitives and data fields to their default states. This ensures that the injection point shared state is properly set up when the shared memory segment is first created.

## Parameters / Member Variables
- : Generic void pointer to the shared memory area that will be cast to 

## Dependencies
- Functions called/Symbols referenced:
  -  (struct type)
  -  (initializes the spinlock for thread-safe access)
  -  (initializes the condition variable for waiting)
- Called from:
  -  (at src/test/modules/injection_points/injection_points.c:126)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the injection_points.c file
- The function initializes three key components of the shared state:
  - A spinlock for thread-safe access to shared data
  - Wait count arrays (zeroed out)
  - Name fields (zeroed out) 
  - A condition variable for coordinating waiting processes
- This function follows PostgreSQL's pattern of using callback functions for shared memory initialization