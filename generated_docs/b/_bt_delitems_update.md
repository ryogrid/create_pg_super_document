# _bt_delitems_update

## Location
[src/backend/access/nbtree/nbtpage.c:1405-1463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1405-L1463)

## Overview
Prepares state needed to update posting list tuples by removing specific TIDs, performing common setup steps for both vacuum and delete operations before their critical sections begin.

## Definition


## Detailed Description
This function sets up the necessary state to delete TIDs from posting list tuples via "updating" the tuple. It performs preparatory steps that are common to both  and  functions, and must be executed before each function's critical section begins.

The function processes each posting list tuple that needs updating by:
1. Calling  to create an updated version of the tuple with specific TIDs removed
2. Building an array of page offset numbers for the updated tuples
3. When WAL logging is needed, constructing a buffer containing  structs that describe the update operations

The original IndexTuple pointers in the  array are replaced with pointers to the final updated versions in palloc'd memory, which the caller must free when done.

## Parameters / Member Variables
- : Array of BTVacuumPosting structures describing tuples to be updated
- : Number of tuples in the updatable array (must be > 0)
- : Output array that receives the page offset numbers for updated tuples
- : Output parameter that receives the final size of the returned buffer
- : Boolean indicating whether WAL logging is required

## Dependencies
- Functions called/Symbols referenced:
  - : Updates posting list tuples by removing specified TIDs
  - : Structure type for vacuum posting list operations
  - : WAL record structure for update operations
  - : Size macro for xl_btree_update structure
  - : Memory allocation function
  - : Memory copy function
- Called from:
  - : Vacuum cleanup operations
  - : Single-page cleanup operations

## Notes and Other Information
- This function must be called before the critical section begins in calling functions
- The function modifies the original  array by replacing IndexTuple pointers with updated versions
- Caller is responsible for freeing the updated tuples when done
- The returned buffer (when WAL logging is needed) contains an array of  structs for WAL record construction
- The  array is populated as a convenience for the caller, even when WAL logging is not required
- Memory allocation for the WAL buffer only occurs when  is true