# contain_nonstrict_functions

## Location
[src/backend/optimizer/util/clauses.c:993-998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L993-L998)

## Overview
Determines whether an expression contains any nonstrict functions that could produce non-NULL output even when given NULL input, used for NULL propagation analysis in query optimization.

## Definition

```c
bool
contain_nonstrict_functions(Node *clause)
```
## Detailed Description
The  function performs a critical analysis for PostgreSQL's query optimizer by determining whether an expression tree contains any nonstrict functions. A strict function is one that always returns NULL when any of its arguments is NULL, while a nonstrict function can return non-NULL values even with NULL inputs.

This analysis is essential for various optimization techniques, particularly:
- **NULL propagation optimization**: Determining if an entire expression will be NULL when certain inputs are NULL
- **Join elimination**: Proving that certain joins can be eliminated because their results would be NULL
- **Predicate pushdown**: Determining if predicates can be safely pushed down through NULL-generating operations

The function delegates the actual tree traversal to , following PostgreSQL's common pattern of having a simple public interface that calls a more complex walker function.

The caller typically uses this function after verifying that the expression contains relevant Var or Param nodes, and wants to prove that the expression result will be NULL if any of these inputs is NULL. If the function returns false, this proof succeeds, enabling various optimizations.

## Parameters / Member Variables
- : The expression tree node to analyze for the presence of nonstrict functions

## Dependencies
- Functions called/Symbols referenced:
  - [contain_nonstrict_functions_walker](contain_nonstrict_functions_walker.md)
- Called from (representative examples):
  - [pullup_replace_vars_callback](../p/pullup_replace_vars_callback.md)
  - [inline_function](../i/inline_function.md)

## Notes and Other Information
- This is a public interface function that provides a clean API for nonstrict function detection
- The function is part of PostgreSQL's broader NULL handling and optimization infrastructure
- A return value of false indicates that the expression is guaranteed to be NULL if any of its variable inputs are NULL, enabling aggressive optimizations
- A return value of true indicates the presence of nonstrict constructs that could produce non-NULL outputs despite NULL inputs, requiring more conservative optimization approaches
- The function is commonly used in conjunction with other clause analysis functions for comprehensive expression evaluation
- Located in src/backend/optimizer/util/clauses.c:993-998