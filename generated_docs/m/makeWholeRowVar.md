# makeWholeRowVar

## Location
[src/backend/nodes/makefuncs.c:135-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L135-L286)

## Overview
Creates a Var node representing a whole-row reference to a range table entry, handling different RTE types and determining the appropriate row type for the variable.

## Definition
```c
Var *makeWholeRowVar(RangeTblEntry *rte, int varno, Index varlevelsup, bool allowScalar)
```

## Detailed Description
The makeWholeRowVar function is a sophisticated constructor that creates Var nodes for whole-row references, which represent entire tuples rather than individual columns. The function analyzes the type of range table entry (relation, subquery, function, join, etc.) and determines the appropriate row type OID to use. For relations, it uses the relation's composite type; for subqueries and functions, it may use RECORD type or specific composite types; and it handles special cases like scalar functions when allowScalar is true. The varattno is always set to InvalidAttrNumber (0) to indicate a whole-row reference.

## Parameters
- `rte`: Pointer to the RangeTblEntry for which to create a whole-row Var
- `varno`: Integer index of the RTE in the range table
- `varlevelsup`: Index indicating nesting level for subquery references (0 for current level, >0 for outer levels)
- `allowScalar`: Boolean flag that allows scalar functions to return their result directly instead of wrapping in a single-column composite

## Dependencies
- Functions called/Symbols referenced:
  - [makeVar](makeVar.md) (creates the actual Var node)
  - [get_rel_type_id](../g/get_rel_type_id.md) (gets composite type OID for relations)
  - [get_rel_name](../g/get_rel_name.md) (gets relation name for error messages)
  - [exprType](../e/exprType.md) (gets type from function expressions)
  - [exprCollation](../e/exprCollation.md) (gets collation from function expressions)
  - [type_is_rowtype](../t/type_is_rowtype.md) (checks if type is composite)
  - [RangeTblEntry](../R/RangeTblEntry.md), RangeTblFunction (struct types)
  - Various RTE kind constants (RTE_RELATION, RTE_SUBQUERY, RTE_FUNCTION)
- Called from (representative examples):
  - [transform_MERGE_to_join](../t/transform_MERGE_to_join.md)
  - [preprocess_targetlist](../p/preprocess_targetlist.md)
  - [expand_inherited_rtentry](../e/expand_inherited_rtentry.md)
  - [transformWholeRowRef](../t/transformWholeRowRef.md)
  - [ApplyRetrieveRule](../A/ApplyRetrieveRule.md)

## Notes and Other Information
- Uses InvalidAttrNumber (0) to signal whole-row references, which is noted as "unclean" but maintained for compatibility
- Handles complex logic for determining row types based on RTE kind and context
- For function RTEs, can return either the function's direct result (if allowScalar=true and function returns scalar) or a composite wrapper
- Supports view expansion and set-returning function contexts in subqueries
- Returns RECORD type for joins, table functions, VALUES, CTEs, and other complex RTE types
- Critical for implementing PostgreSQL's whole-row reference semantics (e.g., "SELECT table_name" vs "SELECT table_name.*")