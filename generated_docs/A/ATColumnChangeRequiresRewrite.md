# ATColumnChangeRequiresRewrite

## Location
src/backend/commands/tablecmds.c: 13099 - 13145

## Overview
Determines whether an ALTER COLUMN TYPE operation requires a table rewrite by analyzing the transformation expression for binary compatibility optimizations.

## Definition


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