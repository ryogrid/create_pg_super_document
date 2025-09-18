# checkWellFormedSelectStmt

## Location
src/backend/parser/parse_cte.c: 1207 - 1270

## Overview
checkWellFormedSelectStmt is a specialized subroutine that validates SELECT statements within recursive CTE contexts, applying different recursion validation rules based on the type of set operation being performed.

## Definition
```c
static void checkWellFormedSelectStmt(SelectStmt *stmt, CteState *cstate)
```

## Detailed Description
This function handles the validation of SELECT statements during recursive CTE well-formedness checking, with particular attention to set operations (UNION, INTERSECT, EXCEPT). It implements context-sensitive validation rules:

**Context-Based Processing:**
- **Non-RECURSION_OK contexts**: Simply recurses through the statement without changing validation state
- **RECURSION_OK context**: Applies specific rules based on the set operation type

**Set Operation Handling:**
- **SETOP_NONE/SETOP_UNION**: Allows normal recursion validation (recursive references permitted)
- **SETOP_INTERSECT**: Changes context to RECURSION_INTERSECT for ALL operations, restricting recursive references
- **SETOP_EXCEPT**: Changes context to RECURSION_EXCEPT, completely prohibiting recursive references in both operands

**Validation Strategy:**
The function carefully controls which parts of the SELECT statement are processed under which recursion contexts. For INTERSECT and EXCEPT operations:
1. Processes left and right operands under restricted contexts
2. Restores original context for processing ORDER BY, LIMIT, OFFSET, and locking clauses
3. Intentionally ignores WITH clauses (handled by parent walker)

This approach ensures that recursive references appear only in semantically valid locations where they can be properly evaluated during recursive query execution.

## Parameters / Member Variables
- `stmt`: The SelectStmt node to validate for recursive CTE well-formedness
- `cstate`: CTE validation state containing current recursion context, item information, and error reporting state

## Dependencies
- Functions called/Symbols referenced:
  - raw_expression_tree_walker (generic tree traversal)
  - checkWellFormedRecursionWalker (specialized recursion walker)
  - elog (internal error logging)
  - SelectStmt (SELECT statement structure)
  - CteState (CTE validation state)
  - RecursionContext (recursion context enum)
  - SETOP_* constants (set operation type enumeration)
  - RECURSION_* constants (recursion context enumeration)

- Called from:
  - checkWellFormedRecursionWalker (main recursion validation walker)

## Notes and Other Information
- This function is essential for handling complex set operations within recursive CTEs where different parts may have different validity rules for recursive references
- The distinction between ALL and non-ALL variants of INTERSECT affects recursion validation (ALL variants are more restrictive)
- EXCEPT operations are particularly restrictive because recursive references in EXCEPT contexts can lead to non-deterministic or incorrect results
- The function carefully preserves and restores recursion contexts to ensure proper validation scope
- WITH clauses are intentionally not processed here since they require special handling by the parent walker function
- The validation rules implemented here ensure that recursive CTEs produce deterministic and correct results according to SQL semantics
- Set operation validation is critical for PostgreSQL's iterative recursive CTE implementation which relies on fixed-point computation