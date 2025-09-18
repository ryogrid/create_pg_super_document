# add_reloption

## Location
src/backend/access/common/reloptions.c: 700 - 733

## Overview
The add_reloption function adds an already-created custom relation option to the global list and triggers recomputation of the main parser table.

## Definition


## Detailed Description
This static function manages the dynamic growth of the custom_options array, which stores pointers to user-defined relation options. The function implements a simple array expansion strategy, starting with an initial capacity of 8 elements and doubling the size when more space is needed.

When adding a new option, the function:
1. Checks if the current custom_options array has sufficient capacity
2. If not, switches to TopMemoryContext and either allocates the initial array or expands the existing one using repalloc
3. Adds the new option pointer to the array and increments the count
4. Sets need_initialization to true, signaling that the main relOpts array needs to be rebuilt to include the new option

The use of TopMemoryContext ensures that custom options persist for the lifetime of the backend process.

## Parameters / Member Variables
- : A pointer to a relopt_gen structure representing the custom relation option to be added

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo
  - palloc
  - repalloc
  - TopMemoryContext
- Global variables accessed:
  - custom_options (array of relopt_gen pointers)
  - num_custom_options (count of custom options)
  - max_custom_options (static capacity tracker)
  - need_initialization (flag for parser table rebuild)
- Called from:
  - add_bool_reloption
  - add_int_reloption  
  - add_real_reloption
  - add_enum_reloption
  - add_string_reloption

## Notes and Other Information
- This is a static function, only accessible within the reloptions.c file
- The function uses a doubling strategy for array growth, starting at 8 elements
- Memory allocation is done in TopMemoryContext to ensure persistence
- Setting need_initialization to true ensures the next call to parseRelOptions will rebuild the unified relOpts array
- The function is called by all the type-specific add_*_reloption wrapper functions
- Custom options are integrated with built-in options during the initialization process