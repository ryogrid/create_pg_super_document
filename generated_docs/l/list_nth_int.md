# list_nth_int

## Location
src/include/nodes/pg_list.h: 310 - 320

## Overview
Returns the integer value contained in the n-th element of an IntList, providing type-safe indexed access to integer values.

## Definition


## Detailed Description
The `list_nth_int` function is a specialized variant of `list_nth` designed specifically for IntList structures that store integer values. It provides type-safe access to integer elements by first verifying that the provided list is actually an IntList using an assertion, then retrieving the integer value at the specified position using `lfirst_int`.

This function ensures type safety by asserting that the list is of type IntList before attempting to access its contents. Like other list access functions, it uses zero-based indexing and provides O(1) access time due to PostgreSQL's array-based list implementation.

## Parameters / Member Variables
- `list`: A const pointer to the List structure, which must be an IntList containing integer values
- `n`: Zero-based index of the integer element to retrieve (must be within bounds: 0 <= n < list->length)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (for type checking to ensure list is IntList)
  - list_nth_cell (to get the cell at position n)
  - lfirst_int (to extract the integer value from the cell)
- Called from (representative examples):
  - CopyReadAttributesText (COPY command attribute processing)
  - CopyReadAttributesCSV (CSV format COPY processing)
  - set_cte_pathlist (CTE path planning)
  - create_ctescan_plan (CTE scan plan creation)
  - rewriteSearchAndCycle (search/cycle rewriting logic)
  - examine_simple_variable (statistics analysis)

## Notes and Other Information
- This is a static inline function for optimal performance
- Specifically designed for IntList structures, not generic Lists
- Uses zero-based indexing (first element is at index 0)
- Type safety is enforced through runtime assertion checking
- No bounds checking beyond what's provided by the underlying list_nth_cell function
- Part of PostgreSQL's type-safe list manipulation API
- Commonly used in query optimization and command processing where integer lists are manipulated
- Calling with a non-IntList or invalid index will result in assertion failure