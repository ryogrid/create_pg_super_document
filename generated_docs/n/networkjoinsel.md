# networkjoinsel

## Location
[src/backend/utils/adt/network_selfuncs.c:196-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L196-L262)

## Overview
Estimates join selectivity for network subnet inclusion/overlap operators, used by PostgreSQL query planner to predict result size when joining tables on network conditions.

## Definition
```c
Datum networkjoinsel(PG_FUNCTION_ARGS)
```

## Detailed Description
The `networkjoinsel` function provides join selectivity estimation for network data type operators (inet, cidr) when used in join conditions. It follows the same structural pattern as `eqjoinsel` but is specialized for network operations like subnet containment and overlap.

The function dispatches to specialized handlers based on join type: `networkjoinsel_inner` for inner/left/full joins, and `networkjoinsel_semi` for semi/anti joins. It includes performance optimizations to handle large statistics by limiting consideration to at most 1024 elements (MAX_CONSIDERED_ELEMS) from MCV and histogram arrays.

For histogram processing, the function uses decimation (sampling every k-th element) to maintain reasonable performance while preserving statistical accuracy. This approach ensures O(N^2) complexity doesn't become prohibitive with large statistics targets.

## Parameters / Member Variables
- `root`: PlannerInfo pointer containing query planning context
- `operator`: OID of the network operator being evaluated
- `args`: List of join arguments
- `jointype`: Type of join operation (unused in current implementation)
- `sjinfo`: SpecialJoinInfo containing join-specific information

## Dependencies
- Functions called/Symbols referenced:
  - [get_join_variables](../g/get_join_variables.md)
  - [networkjoinsel_inner](networkjoinsel_inner.md)
  - [networkjoinsel_semi](networkjoinsel_semi.md)
  - [get_commutator](../g/get_commutator.md)
  - ReleaseVariableStats
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - Used as join selectivity function registered in system catalogs
  - Invoked by query planner during join optimization

## Notes and Other Information
- Treats left/full join selectivity the same as inner join (following eqjoinsel pattern)
- For semi/anti joins, ensures outer variable is passed on the left side
- Uses operator commutation when join variables are reversed
- Implements performance limits to prevent excessive computation with large statistics
- Results are clamped to valid probability range [0.0, 1.0]
- Contains detailed comments about O(N^2) performance considerations and mitigation strategies

## Simplified Source

```c
Datum
networkjoinsel(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid operator = PG_GETARG_OID(1);
    List *args = (List *) PG_GETARG_POINTER(2);
    SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) PG_GETARG_POINTER(4);

    double selec;
    VariableStatData vardata1, vardata2;
    bool join_is_reversed;

    // Extract join variables and determine orientation
    get_join_variables(root, args, sjinfo, &vardata1, &vardata2, &join_is_reversed);

    // Dispatch to appropriate join selectivity handler
    switch (sjinfo->jointype)
    {
        case JOIN_INNER:
        case JOIN_LEFT:
        case JOIN_FULL:
            // Inner/left/full joins use same selectivity calculation
            selec = networkjoinsel_inner(operator, &vardata1, &vardata2);
            break;

        case JOIN_SEMI:
        case JOIN_ANTI:
            // Semi/anti joins require outer variable on left side
            if (!join_is_reversed)
                selec = networkjoinsel_semi(operator, &vardata1, &vardata2);
            else
                selec = networkjoinsel_semi(get_commutator(operator), &vardata2, &vardata1);
            break;

        default:
            elog(ERROR, "unrecognized join type: %d", (int) sjinfo->jointype);
            selec = 0;
            break;
    }

    ReleaseVariableStats(vardata1);
    ReleaseVariableStats(vardata2);
    CLAMP_PROBABILITY(selec);

    PG_RETURN_FLOAT8((float8) selec);
}
```