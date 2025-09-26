# checkWellFormedRecursion

## Location
[src/backend/parser/parse_cte.c:915-1026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_cte.c#L915-L1026)

## Overview
checkWellFormedRecursion validates that recursive Common Table Expressions (CTEs) conform to PostgreSQL's strict requirements for recursive query structure and content.

## Definition
```c
static void checkWellFormedRecursion(CteState *cstate)
```

## Detailed Description
This function enforces PostgreSQL's recursive CTE validation rules by checking each recursive CTE in the dependency analysis state. The validation ensures recursive queries follow the required pattern and restrictions:

**Required Structure:**
- Must be a SELECT statement (no data-modifying statements like INSERT/UPDATE/DELETE)
- Must have a top-level UNION operation
- Must follow the pattern: non-recursive-term UNION [ALL] recursive-term

**Prohibited Features:**
- ORDER BY clauses (not implementable with partial results)
- LIMIT/OFFSET clauses (not implementable with partial results)  
- FOR UPDATE/SHARE locking clauses (not implementable with partial results)
- Self-references in top-level WITH clauses

**Recursive Reference Validation:**
- Left-hand (non-recursive) term: Must contain zero self-references
- Right-hand (recursive) term: Must contain exactly one self-reference in a valid context
- Self-references must appear in appropriate contexts (not in sublinks, aggregates, etc.)

The function uses checkWellFormedRecursionWalker to perform the actual tree traversal and reference counting for different parts of the recursive query.

## Parameters / Member Variables
- `cstate`: CTE state containing all items to validate, current item context, recursion tracking counters, and parse state for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [checkWellFormedRecursionWalker](checkWellFormedRecursionWalker.md) (recursive tree walker for validation)
  - ereport (error reporting)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - [parser_errposition](../p/parser_errposition.md) (parse location for errors)
  - [exprLocation](../e/exprLocation.md) (expression location tracking)
  - IsA (type checking macro)
  - Assert (assertion macro)
  - elog (internal error logging)
  - CommonTableExpr (CTE structure)
  - [SelectStmt](../S/SelectStmt.md) (SELECT statement structure)
  - SETOP_UNION (union operation constant)
  - RECURSION_* constants (recursion context enums)

- Called from:
  - [transformWithClause](../t/transformWithClause.md) (main WITH clause transformation)

## Notes and Other Information
- Recursive CTEs in PostgreSQL must follow SQL standard requirements with additional restrictions for implementation feasibility
- The function validates structure before semantic analysis to provide clear error messages
- Self-reference counting prevents both missing recursion (infinite loops) and multiple recursion (undefined semantics)
- The validation is performed on raw parse trees before query analysis/transformation
- Error messages include specific parse locations to help users identify problematic constructs
- The function handles nested WITH clauses but prohibits self-references within them for recursive CTEs
- PostgreSQL's recursive CTE implementation is based on iteration rather than true recursion, requiring these structural constraints