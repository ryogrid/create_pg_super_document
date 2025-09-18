# bms_add_member

## Location
src/backend/nodes/bitmapset.c: 815 - 867

## Overview
Adds a specified member to a bitmapset, automatically expanding the set's storage if necessary and recycling the input when possible.

## Definition


## Detailed Description
The  function adds a specified integer member  to bitmapset . If the input set is NULL, it creates a new singleton set. If the member being added requires more storage space than currently allocated, the function automatically expands the bitmapset using . The function follows PostgreSQL's memory management pattern by recycling the input bitmapset when possible, returning the potentially modified bitmapset pointer.

The function calculates which word and bit position the new member should occupy using the  and  macros, then sets the appropriate bit using bitwise OR operations. When expanding storage, it properly initializes new words to zero.

## Parameters / Member Variables
- : Input Bitmapset to modify (can be NULL, which creates a new singleton set)
- : Integer member to add to the set (must be non-negative)

## Dependencies
- Functions called/Symbols referenced:
  - : Validates the input bitmapset structure
  - : Creates a new single-member set when input is NULL
  - : Macro to calculate which bitmap word contains the member
  - : Macro to calculate bit position within the word
  - : Macro to calculate memory size needed for given word count
  - : PostgreSQL memory reallocation function
  - : Type for individual bitmap storage words
  - : Optional function for memory management (when REALLOCATE_BITMAPSETS is defined)
- Called from (representative examples):
  - : Column mapping during tuple conversion
  - : Constraint attribute number collection
  - : Executor projection building
  - : Column detection in aggregation nodes
  - : Equivalence class processing
  - : Index usage classification
  - : Outer join information creation

## Notes and Other Information
- Returns an error for negative member values using 
- Automatically handles memory expansion when adding members beyond current capacity
- The input bitmapset pointer may be invalidated and a new pointer returned
- Initializes newly allocated words to zero to maintain clean state
- Under  compile flag, performs additional copy-and-free for memory safety
- Widely used throughout PostgreSQL for building sets of column numbers, relation IDs, and other integer collections
- Essential function for dynamic bitmapset construction in query planning and execution