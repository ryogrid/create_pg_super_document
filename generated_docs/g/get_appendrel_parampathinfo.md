# get_appendrel_parampathinfo

## Location
[src/backend/optimizer/util/relnode.c:1868-1900](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L1868-L1900)

## Overview
Get the ParamPathInfo for a parameterized path for an append relation, creating a minimal structure that flags the need for parameters without rowcount estimation or clause handling.

## Definition

```c
ParamPathInfo *
get_appendrel_parampathinfo(RelOptInfo *appendrel, Relids required_outer)
```
## Detailed Description
This function creates a ParamPathInfo structure for parameterized append relations. Unlike base relations and joins, append relations don't need detailed rowcount estimation or clause handling in their ParamPathInfo, since the Append node itself doesn't evaluate qualifications and rowcounts are computed as the sum of child estimates.

The function creates a minimal ParamPathInfo with zero ppi_rows and empty ppi_clauses, serving primarily as a flag to indicate that the path requires parameterization. The actual rowcount estimation for append paths is handled elsewhere by summing estimates from the child relations.

## Parameters / Member Variables
- : RelOptInfo structure representing the append relation
- : Relids bitmap specifying the outer relations that must be available for parameter values

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_subset](../b/bms_is_subset.md)
  - bms_is_empty
  - [bms_overlap](../b/bms_overlap.md)
  - [find_param_path_info](../f/find_param_path_info.md)
  - makeNode (ParamPathInfo)
- Called from (representative examples):
  - [create_append_path](../c/create_append_path.md)
  - [create_merge_append_path](../c/create_merge_append_path.md)

## Notes and Other Information
- Returns NULL for unparameterized paths (when required_outer is empty)
- Creates ParamPathInfo with ppi_rows set to 0, as rowcount is computed from children
- Sets ppi_clauses to NIL since Append nodes don't evaluate qualifications
- Asserts that LATERAL references are properly accounted for in required_outer
- Much simpler than base relation and join parameterization due to append semantics
- The function is located in src/backend/optimizer/util/relnode.c:1868-1900