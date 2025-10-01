# calc_nestloop_required_outer

## Location
[src/backend/optimizer/util/pathnode.c:2378-2404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2378-L2404)

## Overview
Computes the required outer relation set for a nested loop join path by determining which relations must be available as parameters from outside the join.

## Definition
```c
Relids calc_nestloop_required_outer(Relids outerrelids,
                                   Relids outer_paramrels,
                                   Relids innerrelids,
                                   Relids inner_paramrels)
```

## Detailed Description
This function calculates the set of relations that must be provided as parameters from outside a nested loop join. It handles the parameter dependencies between the outer and inner sides of the join, ensuring that the inner path can receive parameters from the outer path but not vice versa. The function combines parameter requirements from both sides and removes relations that will be satisfied by the outer side of the join. The result must not share storage with the input parameters to avoid memory corruption.

## Parameters / Member Variables
- `outerrelids`: Set of relation IDs that will be provided by the outer side of the join
- `outer_paramrels`: Set of relation IDs that the outer path requires as parameters
- `innerrelids`: Set of relation IDs that will be provided by the inner side of the join
- `inner_paramrels`: Set of relation IDs that the inner path requires as parameters

## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md) (to check for invalid parameter dependencies)
  - [bms_copy](../b/bms_copy.md) (to copy parameter sets)
  - [bms_union](../b/bms_union.md) (to combine parameter requirements)
  - [bms_del_members](../b/bms_del_members.md) (to remove satisfied parameters)
  - Assert (for debugging parameter constraint validation)
- Called from (representative examples):
  - [try_nestloop_path](../t/try_nestloop_path.md) (in joinpath.c)

## Notes and Other Information
- The function enforces the constraint that outer paths cannot depend on inner relations through an assertion
- When the inner path is not parameterized, the function simply returns a copy of the outer path's parameter requirements
- The function uses top-level parent relation IDs even when considering child joins for consistency
- Memory management is important: the result is allocated separately and does not share storage with inputs
- This is a core utility function for nested loop join planning in the PostgreSQL query optimizer

## Simplified Source

```c
Relids
calc_nestloop_required_outer(Relids outerrelids,
                             Relids outer_paramrels,
                             Relids innerrelids,
                             Relids inner_paramrels)
{
    Relids required_outer;

    // Validate: inner path can require outer rels, but not vice versa
    Assert(!bms_overlap(outer_paramrels, innerrelids));

    // Simple case: inner path not parameterized
    if (!inner_paramrels)
        return bms_copy(outer_paramrels);

    // Combine parameter requirements from both sides
    required_outer = bms_union(outer_paramrels, inner_paramrels);

    // Remove parameters that will be satisfied by the outer relation
    required_outer = bms_del_members(required_outer, outerrelids);

    return required_outer;
}
```