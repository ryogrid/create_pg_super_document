# IsBooleanOpfamily

## Location
src/backend/optimizer/path/indxpath.c: 2280 - 2304

## Overview
Detects whether an operator family supports boolean equality operations, enabling special handling of boolean index columns in query optimization.

## Definition
```c
static bool
IsBooleanOpfamily(Oid opfamily)
```

## Detailed Description
This function determines if a given operator family can handle boolean equality operations. It uses a two-tier approach for efficiency:

1. **Built-in opfamilies**: For operator families with OIDs in the built-in range (< FirstNormalObjectId), it uses hardwired knowledge via `IsBuiltinBooleanOpfamily` for fast lookup without catalog access.

2. **Extension opfamilies**: For user-defined or extension operator families, it performs a catalog lookup using `op_in_opfamily` to check if the `BooleanEqualOperator` is supported.

This distinction is critical for performance optimization in query planning, as boolean indexes can use specialized matching strategies that differ from regular operator-based index matching.

## Parameters / Member Variables
- `opfamily`: OID of the operator family to test for boolean equality support

## Dependencies
- Functions called/Symbols referenced:
  - FirstNormalObjectId
  - IsBuiltinBooleanOpfamily
  - [op_in_opfamily](../o/op_in_opfamily.md)
  - BooleanEqualOperator (constant)
- Called from (representative examples):
  - ec_member_matches_arg
  - [match_clause_to_indexcol](../m/match_clause_to_indexcol.md)
  - [indexcol_is_bool_constant_for_query](../i/indexcol_is_bool_constant_for_query.md)

## Notes and Other Information
- Performance optimization: avoids catalog lookups for built-in opfamilies
- Enables special boolean index clause matching in `match_clause_to_indexcol`
- Part of the index path selection optimization in PostgreSQL's query planner
- Located in `src/backend/optimizer/path/indxpath.c:2280-2304`
- Returns boolean result indicating whether the opfamily supports boolean equality operations