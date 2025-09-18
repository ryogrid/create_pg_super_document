# intset_create

## Location
src/backend/lib/integerset.c: 284 - 315

## Overview
Creates a new, initially empty IntegerSet data structure for storing compressed 64-bit integers in PostgreSQL.

## Definition


## Detailed Description
The  function allocates and initializes a new IntegerSet structure in the current memory context. The function sets up an empty B-tree-like structure with all fields initialized to their default values. All subsequent memory allocations for this IntegerSet will be performed in the same memory context that was current when this function was called, regardless of which memory context is active when new integers are added to the set.

The function initializes the IntegerSet with:
- Zero entries and no highest value recorded
- Empty B-tree structure (no levels, no root node)
- No buffered values awaiting insertion
- Inactive iterator state

## Parameters / Member Variables
(No parameters - this function takes no arguments)

## Dependencies
- Functions called/Symbols referenced:
  - : Memory allocation function
  - : Global variable for current memory context
  - : Function to get allocated memory size
  - : Standard C library function for memory initialization
- Called from (representative examples):
  - : Used in GiST index vacuum operations
  - Various test functions in test_integerset module

## Notes and Other Information
- The IntegerSet is created in the current memory context and will continue to use that context for all future allocations
- The structure is initialized as completely empty with no buffered values or tree nodes
- Memory usage tracking begins immediately with the initial allocation size
- Iterator state is initialized as inactive and ready for use