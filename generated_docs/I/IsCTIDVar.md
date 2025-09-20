# IsCTIDVar

## Location
[src/backend/optimizer/path/tidpath.c:55-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/tidpath.c#L55-L75)

## Overview
IsCTIDVar is a macro that checks whether a given node represents a CTID (Current Tuple Identifier) variable in PostgreSQL's executor.

## Definition

```c
static inline bool
IsCTIDVar(Var *var, RelOptInfo *rel)
```
## Detailed Description
This macro provides a convenient way to identify CTID variables during query execution. It performs three checks: ensures the node is not NULL, verifies it's a Var node type, and confirms that the variable's attribute number matches SelfItemPointerAttributeNumber (which represents the CTID column). The macro is designed specifically for relation scan qualifiers where any Var must belong to the current table being scanned. Parameters from other tables would have been converted to Param nodes by the time this check is performed.

## Parameters / Member Variables
- : The node to be tested for being a CTID variable

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - SelfItemPointerAttributeNumber (constant defining CTID attribute number)
- Called from (representative examples):
  - [MakeTidOpExpr](../M/MakeTidOpExpr.md)
  - [TidExprListCreate](../T/TidExprListCreate.md)
  - [IsBinaryTidClause](IsBinaryTidClause.md)
  - [IsTidEqualAnyClause](IsTidEqualAnyClause.md)
  - [ec_member_matches_ctid](../e/ec_member_matches_ctid.md)

## Notes and Other Information
The macro includes a detailed comment explaining that checking varattno is sufficient to identify CTID variables because any Var in the relation scan qual must belong to the current table. This is guaranteed by PostgreSQL's parameter handling, where variables from other tables would have been converted to Param nodes during query planning.