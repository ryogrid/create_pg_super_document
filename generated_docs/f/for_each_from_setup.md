# for_each_from_setup

## Location
src/include/nodes/pg_list.h: 423 - 437

## Overview
Initializes a ForEachState structure for the for_each_from macro, enabling iteration through a PostgreSQL List starting from a specified position.

## Definition


## Detailed Description
The `for_each_from_setup` function is a helper function that initializes the state required for the `for_each_from` macro. It creates and returns a `ForEachState` structure that contains the list pointer and the starting index for iteration. This function is used internally by the `for_each_from` macro to set up iteration that begins from an arbitrary position within the list rather than from the beginning.

The function performs a basic validation check to ensure the starting index is non-negative, though it allows the index to exceed the list length (which is handled by the calling macro).

## Parameters / Member Variables
- `lst`: A pointer to the PostgreSQL List structure to iterate over
- `N`: The zero-based starting index for iteration (must be >= 0)

## Dependencies
- Functions called/Symbols referenced:
  - [ForEachState](../F/ForEachState.md) (struct type)
  - Assert (for validation)
- Called from (representative examples):
  - for_each_from (macro)

## Notes and Other Information
- This is an inline function for performance optimization
- Used internally by the `for_each_from` macro, not typically called directly by user code
- The function allows N to exceed the list length, relying on the calling macro to handle bounds checking
- Part of PostgreSQL's list iteration infrastructure
- Returns a ForEachState struct by value, containing the list pointer and starting index
- The Assert ensures N is non-negative, preventing invalid negative indexing