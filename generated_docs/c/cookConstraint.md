# cookConstraint

## Location
[src/backend/catalog/heap.c:2883-2920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L2883-L2920)

## Overview
Transforms raw CHECK constraint expressions into cooked format ready for storage, ensuring the expression yields a boolean result and references only the target table.

## Definition
static Node *cookConstraint(ParseState *pstate, Node *raw_constraint, char *relname)

## Detailed Description
This static function processes CHECK constraint expressions during table creation or constraint addition. It converts raw parse tree representations into executable expressions that can be stored in the system catalogs and evaluated during data modification operations. 

The function performs several critical validations:
1. Transforms the raw expression into an executable format
2. Ensures the expression evaluates to a boolean result (as required for CHECK constraints)
3. Handles collation assignments for proper string comparisons
4. Validates that only the target table is referenced in the constraint expression

The function is part of PostgreSQL's constraint validation system and ensures that CHECK constraints are properly formed and contain only valid references before being stored in the system catalogs.

## Parameters / Member Variables
- `pstate`: ParseState containing parser context, range table, and error reporting information
- `raw_constraint`: The raw Node representing the unparsed CHECK constraint expression
- `relname`: Name of the target relation for error reporting when invalid references are found

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](../t/transformExpr.md): Converts raw parse tree to executable expression with CHECK_CONSTRAINT context
  - [coerce_to_boolean](coerce_to_boolean.md): Ensures the expression returns a boolean value
  - [assign_expr_collations](../a/assign_expr_collations.md): Resolves collation assignments in the expression
  - [list_length](../l/list_length.md): Checks the number of relations in the range table
  - ereport: Reports errors for invalid column references

- Called from (representative examples):
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md): During table creation or constraint addition operations

## Notes and Other Information
- This is a static function used internally within heap.c for constraint processing
- The function enforces that CHECK constraints can only reference columns from the target table
- The range table validation (p_rtable length check) prevents references to external relations
- [Boolean](../B/Boolean.md) coercion ensures that CHECK constraints properly evaluate to true/false values
- Collation handling ensures string comparisons work correctly within the constraint
- The comment mentions that some validation may be "dead code" due to historical changes in query processing
- CHECK constraints are fundamental to PostgreSQL's data integrity enforcement system