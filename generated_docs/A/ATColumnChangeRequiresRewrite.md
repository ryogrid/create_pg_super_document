# ATColumnChangeRequiresRewrite

## Location
[src/backend/commands/tablecmds.c:13099-13145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L13099-L13145)

## Overview
Determines whether an ALTER COLUMN TYPE operation requires a table rewrite by analyzing the transformation expression for binary compatibility optimizations.

## Definition

```c
static bool
ATColumnChangeRequiresRewrite(Node *expr, AttrNumber varattno)
```
## Detailed Description
This function analyzes the transformation expression used in ALTER COLUMN TYPE to determine if a table rewrite can be avoided. It recursively examines the expression tree looking for patterns that indicate the transformation is sufficiently simple that existing data can be used without rewriting. The function recognizes several safe transformation patterns: direct variable references (no transformation), RelabelType nodes (binary-compatible type changes), unconstrained domain coercions, and specific timestamp/timestamptz conversions when the timezone is UTC. If any of these optimizable patterns are found, the function returns false to indicate no rewrite is needed. All other transformations require a full table rewrite.

## Parameters / Member Variables
- : The transformation expression to analyze (typically the USING clause or coercion expression)
- : The attribute number of the column being altered

## Dependencies
- Functions called/Symbols referenced:
  - IsA (node type checking for Var, RelabelType, CoerceToDomain, FuncExpr)
  - [DomainHasConstraints](../D/DomainHasConstraints.md) (domain constraint checking)
  - [TimestampTimestampTzRequiresRewrite](../T/TimestampTimestampTzRequiresRewrite.md) (timezone-specific rewrite determination)
  - linitial (list access for function arguments)
- Called from (representative examples):
  - [ATPrepAlterColumnType](ATPrepAlterColumnType.md) (rewrite determination during ALTER COLUMN TYPE preparation)

## Notes and Other Information
- Optimization function that can significantly improve ALTER TABLE performance
- Safe transformations that avoid rewrite: binary coercible types, unconstrained domains, UTC timestamp conversions
- Uses recursive descent to analyze complex expression trees
- Constrained domains always require rewrite for constraint validation
- Timestamp/timestamptz conversions are optimizable only when timezone is UTC
- Returns true (rewrite required) for any unrecognized transformation patterns
- Critical for performance of large table alterations in PostgreSQL
- Part of the ALTER TABLE optimization infrastructure

## Simplified Source

```c
static bool ATColumnChangeRequiresRewrite(Node *expr, AttrNumber varattno) {
    Assert(expr != NULL);

    // Recursively analyze the transformation expression
    for (;;) {
        if (IsA(expr, Var) && ((Var *) expr)->varattno == varattno) {
            // Direct column reference - no transformation needed
            return false;
        }
        else if (IsA(expr, RelabelType)) {
            // Binary-compatible type conversion - unwrap and continue
            expr = (Node *) ((RelabelType *) expr)->arg;
        }
        else if (IsA(expr, CoerceToDomain)) {
            // Domain coercion
            CoerceToDomain *d = (CoerceToDomain *) expr;

            if (DomainHasConstraints(d->resulttype)) {
                // Constrained domain requires rewrite for validation
                return true;
            }
            // Unconstrained domain - continue analysis
            expr = (Node *) d->arg;
        }
        else if (IsA(expr, FuncExpr)) {
            // Function call - check for special timestamp conversions
            FuncExpr *f = (FuncExpr *) expr;

            switch (f->funcid) {
                case F_TIMESTAMPTZ_TIMESTAMP:
                case F_TIMESTAMP_TIMESTAMPTZ:
                    // Timestamp conversions are safe only in UTC timezone
                    if (TimestampTimestampTzRequiresRewrite()) {
                        return true;
                    } else {
                        expr = linitial(f->args);
                        break;
                    }
                default:
                    // Any other function requires rewrite
                    return true;
            }
        }
        else {
            // Unrecognized expression type requires rewrite
            return true;
        }
    }
}
```