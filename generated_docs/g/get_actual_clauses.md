# get_actual_clauses

## Location
src/backend/optimizer/util/restrictinfo.c: 469 - 493

## Overview
Extracts bare clause expressions from a list of RestrictInfo structures, specifically designed for cases where no pseudoconstant clauses are expected.

## Definition
```c
List *get_actual_clauses(List *restrictinfo_list)
```

## Detailed Description
This function serves as a specialized clause extraction utility that converts a list of RestrictInfo wrappers into a list of their underlying clause expressions. It is designed for use in contexts where the caller can guarantee that the input list contains no pseudoconstant clauses (such as index qualification lists).

The function includes assertions to verify these preconditions: it checks that no RestrictInfo in the input list is marked as pseudoconstant and that none represents a constant TRUE expression. These assertions help catch programming errors where the function is used inappropriately.

## Parameters / Member Variables
- `restrictinfo_list`: Input list of RestrictInfo pointers from which to extract clause expressions

## Dependencies
- Functions called/Symbols referenced:
  - rinfo_is_constant_true (Line 479) - to verify no constant TRUE clauses
  - lfirst_node macro - for safe list iteration
  - lappend - to build the result list
  - NIL - PostgreSQL's empty list constant
- Called from (representative examples):
  - create_join_plan (src/backend/optimizer/plan/createplan.c:1128)
  - create_bitmap_subplan (src/backend/optimizer/plan/createplan.c:3500)
  - create_mergejoin_plan (src/backend/optimizer/plan/createplan.c:4501)
  - create_hashjoin_plan (src/backend/optimizer/plan/createplan.c:4803)
  - make_simple_restrictinfo (src/include/optimizer/restrictinfo.h:39)

## Notes and Other Information
- This function is specifically intended for use with "clean" RestrictInfo lists where pseudoconstants have already been filtered out
- The assertions serve as both documentation and debugging aids, clearly indicating the function's preconditions
- Unlike extract_actual_clauses, this function does not perform filtering - it assumes the input is already clean
- Commonly used during plan creation phases where index qualifications or join clauses need to be converted from their RestrictInfo wrappers to bare expressions
- The function maintains the same order as the input list while extracting clause expressions