# NewColumnValue

## Location
[src/backend/commands/tablecmds.c:231-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L231-L237)

## Overview
NewColumnValue is a structure used during PostgreSQL's ALTER TABLE operations to represent column values that need to be computed during the Phase 3 table rewrite. It handles both new columns with defaults and columns undergoing type changes.

## Definition
```c
typedef struct NewColumnValue
{
    AttrNumber  attnum;      /* which column */
    Expr       *expr;        /* expression to compute */
    ExprState  *exprstate;   /* execution state */
    bool        is_generated; /* is it a GENERATED expression? */
} NewColumnValue;
```

## Detailed Description
NewColumnValue is used during the Phase 3 copy operation of ALTER TABLE when a table rewrite is required. It represents column values that cannot simply be copied from the old table but must be computed. This includes new columns with non-null default values and existing columns that are changing data types. The structure stores both the expression to compute the new value and its compiled execution state for efficient evaluation. For regular columns, expressions operate over old table values, while for generated columns, expressions operate over the new tuple's column values. Columns without a NewColumnValue entry are simply copied unchanged during the table rewrite.

## Parameters / Member Variables
- `attnum`: The attribute number (column position) in the new table layout
- `expr`: The expression that computes the new column value
- `exprstate`: Compiled execution state for the expression (for efficient evaluation)
- `is_generated`: Flag indicating whether this is a GENERATED column expression

## Dependencies
- Functions called/Symbols referenced:
  - Standard PostgreSQL types (AttrNumber, Expr, ExprState)
- Called from (representative examples):
  - [ATRewriteTable](../A/ATRewriteTable.md)
  - [ATExecAddColumn](../A/ATExecAddColumn.md)
  - [ATExecSetExpression](../A/ATExecSetExpression.md)
  - [ATPrepAlterColumnType](../A/ATPrepAlterColumnType.md)

## Notes and Other Information
- Used specifically during Phase 3 table rewrite operations in ALTER TABLE
- Expressions for regular columns operate over old table values
- Expressions for generated columns operate over new tuple column values
- Only needed for columns that require computation; others are simply copied
- The exprstate is compiled for efficient row-by-row evaluation during table rewrite
- Essential for handling ADD COLUMN with DEFAULT and ALTER COLUMN TYPE operations