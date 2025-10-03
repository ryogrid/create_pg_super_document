# makeBoolConst

## Location
[src/backend/nodes/makefuncs.c:406-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L406-L417)

## Overview
Creates a Const node representing a boolean value, which can also represent NULL boolean values.

## Definition

```c
Node *
makeBoolConst(bool value, bool isnull)
```
## Detailed Description
The  function is a utility function that creates a Const node specifically for boolean values. It leverages the generic  function but provides specialized handling for boolean types. The function hardwires the boolean size as 1 byte (as defined in pg_type.h) and uses the BOOLOID type identifier. This function is essential for creating boolean constant expressions throughout the PostgreSQL query processing pipeline.

## Parameters / Member Variables
- `value`: The boolean value to be stored in the constant node (true or false)
- `isnull`: Flag indicating whether the constant represents a NULL value
## Dependencies
- Functions called/Symbols referenced:
  - [makeConst](makeConst.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - BOOLOID (constant)
  - InvalidOid (constant)
- Called from (representative examples):
  - [make_ands_explicit](make_ands_explicit.md)
  - [reconsider_outer_join_clauses](../r/reconsider_outer_join_clauses.md)
  - [match_boolean_index_clause](match_boolean_index_clause.md)
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md)
  - [negate_clause](../n/negate_clause.md)

## Notes and Other Information
- The function hardwires the boolean size as 1 byte, duplicating the definition from pg_type.h
- Returns a Node pointer that can be cast to Const when needed
- Used extensively throughout the optimizer and parser for creating boolean constant expressions
- The function handles both regular boolean values and NULL boolean values through the isnull parameter

## Simplified Source

```c
Node *makeBoolConst(bool value, bool isnull) {
    // Create a Const node for boolean values
    // Boolean size is hardwired as 1 byte (from pg_type.h)
    return (Node *) makeConst(BOOLOID, -1, InvalidOid, 1,
                             BoolGetDatum(value), isnull, true);
}
```