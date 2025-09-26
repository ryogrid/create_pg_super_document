# markNullableIfNeeded

## Location
[src/backend/parser/parse_relation.c:1035-1065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L1035-L1065)

## Overview
Marks a Var node as nullable if the referenced RTE is made nullable by outer joins at the current point in the query.

## Definition

```c
union(var->varnullingrels, relids);
```
## Detailed Description
The `markNullableIfNeeded` function determines whether a Var node should be marked as nullable due to outer join semantics. When a table is referenced in an outer join, columns from that table may become nullable even if they are normally NOT NULL. This function examines the parse state's nulling relations to determine if the RTE referenced by the Var is affected by outer joins at the current query level.

The function first navigates to the appropriate parse state level based on the Var's `varlevelsup` field, then looks up the nulling relations for the Var's relation in the `p_nullingrels` list. If nulling relations are found, they are merged with any existing nulling relations already stored in the Var's `varnullingrels` field using bitmap set union operations.

## Parameters / Member Variables
- `pstate`: The current parse state containing nulling relation information
- `var`: The Var node to potentially mark as nullable

## Dependencies
- Functions called/Symbols referenced:
  - [list_nth](../l/list_nth.md)
  - [bms_union](../b/bms_union.md)
- Called from (representative examples):
  - [buildVarFromNSColumn](../b/buildVarFromNSColumn.md)
  - [transformWholeRowRef](../t/transformWholeRowRef.md)
  - [scanNSItemForColumn](../s/scanNSItemForColumn.md)
  - [expandNSItemVars](../e/expandNSItemVars.md)

## Notes and Other Information
- The function modifies the Var's `varnullingrels` field in place
- Handles multi-level query nesting by traversing parent parse states
- Uses bitmap sets to efficiently represent sets of relation IDs that make this Var nullable
- The nulling relations are determined during join processing and stored in the parse state's `p_nullingrels` list
- This is crucial for correct NULL semantics in outer joins, ensuring that expressions properly handle potentially NULL values from outer-joined tables