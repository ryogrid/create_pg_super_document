# eqjoinsel

## Location
[src/backend/utils/adt/selfuncs.c:2273-2437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2273-L2437)

## Overview
Calculates join selectivity for equality ("=") operators, serving as the core PostgreSQL function for estimating how many rows will result from equality-based joins between relations.

## Definition

```c
Datum
eqjoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
This PostgreSQL function estimates the selectivity of equality joins by analyzing statistics from both sides of the join condition. The function handles different join types (INNER, LEFT, FULL, SEMI, ANTI) and uses sophisticated statistical analysis including Most Common Values (MCVs) when available.

The estimation process involves:
1. **Variable analysis**: Extracting statistical information from both join variables using 
2. **Distinct value estimation**: Computing the number of distinct values using 
3. **MCV statistics**: Fetching and analyzing Most Common Values when security checks pass
4. **Join-type specific computation**: Delegating to specialized functions (, ) based on join type
5. **Result clamping**: Ensuring semi/anti join selectivity doesn't exceed inner join selectivity

For SEMI and ANTI joins, the function ensures logical consistency by clamping the result to not exceed what an equivalent inner join would produce.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : PlannerInfo structure with planner context
  - : OID of the equality operator
  - : List containing the two join arguments
  - : Type of join operation (currently unused but preserved)
  - : SpecialJoinInfo containing join metadata
  - : Collation information for the comparison

## Dependencies
- Functions called/Symbols referenced:
  - [get_join_variables](../g/get_join_variables.md)
  - [get_variable_numdistinct](../g/get_variable_numdistinct.md)
  - [get_opcode](../g/get_opcode.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [statistic_proc_security_check](../s/statistic_proc_security_check.md)
  - [eqjoinsel_inner](eqjoinsel_inner.md)
  - [eqjoinsel_semi](eqjoinsel_semi.md)
  - [find_join_input_rel](../f/find_join_input_rel.md)
  - [get_commutator](../g/get_commutator.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - ReleaseVariableStats
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - [neqjoinsel](../n/neqjoinsel.md)

## Notes and Other Information
- The function requires valid statistics tuples from both sides to effectively use MCV (Most Common Values) analysis
- Security checks are performed before accessing detailed statistics to prevent information leakage
- For SEMI/ANTI joins with reversed arguments, the function finds the commutator operator and swaps arguments appropriately
- The result is clamped to ensure semi-join selectivity never exceeds inner-join selectivity for the same conditions
- Memory management is carefully handled with proper cleanup of statistics slots and variable stats
- The function returns a float8 value representing the estimated selectivity (fraction of qualifying rows)

## Simplified Source

```c
Datum
eqjoinsel(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid operator = PG_GETARG_OID(1);
    List *args = (List *) PG_GETARG_POINTER(2);
    SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) PG_GETARG_POINTER(4);
    Oid collation = PG_GET_COLLATION();

    double selec;
    double selec_inner;
    VariableStatData vardata1, vardata2;
    double nd1, nd2;
    bool isdefault1, isdefault2;
    Oid opfuncoid;
    AttStatsSlot sslot1, sslot2;
    Form_pg_statistic stats1 = NULL, stats2 = NULL;
    bool have_mcvs1 = false, have_mcvs2 = false;
    bool get_mcv_stats;
    bool join_is_reversed;
    RelOptInfo *inner_rel;

    // Extract join variables and their statistics
    get_join_variables(root, args, sjinfo, &vardata1, &vardata2, &join_is_reversed);

    // Get distinct value estimates for both sides
    nd1 = get_variable_numdistinct(&vardata1, &isdefault1);
    nd2 = get_variable_numdistinct(&vardata2, &isdefault2);

    opfuncoid = get_opcode(operator);

    // Initialize statistics slots
    memset(&sslot1, 0, sizeof(sslot1));
    memset(&sslot2, 0, sizeof(sslot2));

    // Check if we can use MCV (Most Common Values) statistics
    get_mcv_stats = (HeapTupleIsValid(vardata1.statsTuple) &&
                     HeapTupleIsValid(vardata2.statsTuple) &&
                     get_attstatsslot(&sslot1, vardata1.statsTuple, STATISTIC_KIND_MCV, InvalidOid, 0) &&
                     get_attstatsslot(&sslot2, vardata2.statsTuple, STATISTIC_KIND_MCV, InvalidOid, 0));

    // Retrieve statistics with security checks
    if (HeapTupleIsValid(vardata1.statsTuple)) {
        stats1 = (Form_pg_statistic) GETSTRUCT(vardata1.statsTuple);
        if (get_mcv_stats && statistic_proc_security_check(&vardata1, opfuncoid))
            have_mcvs1 = get_attstatsslot(&sslot1, vardata1.statsTuple, STATISTIC_KIND_MCV,
                                        InvalidOid, ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);
    }

    if (HeapTupleIsValid(vardata2.statsTuple)) {
        stats2 = (Form_pg_statistic) GETSTRUCT(vardata2.statsTuple);
        if (get_mcv_stats && statistic_proc_security_check(&vardata2, opfuncoid))
            have_mcvs2 = get_attstatsslot(&sslot2, vardata2.statsTuple, STATISTIC_KIND_MCV,
                                        InvalidOid, ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);
    }

    // Calculate inner-join selectivity (used by all join types)
    selec_inner = eqjoinsel_inner(opfuncoid, collation, &vardata1, &vardata2,
                                 nd1, nd2, isdefault1, isdefault2,
                                 &sslot1, &sslot2, stats1, stats2,
                                 have_mcvs1, have_mcvs2);

    // Handle different join types
    switch (sjinfo->jointype) {
        case JOIN_INNER:
        case JOIN_LEFT:
        case JOIN_FULL:
            selec = selec_inner;
            break;

        case JOIN_SEMI:
        case JOIN_ANTI:
            // Find the inner relation for semi/anti joins
            inner_rel = find_join_input_rel(root, sjinfo->min_righthand);

            if (!join_is_reversed) {
                selec = eqjoinsel_semi(opfuncoid, collation, &vardata1, &vardata2,
                                     nd1, nd2, isdefault1, isdefault2,
                                     &sslot1, &sslot2, stats1, stats2,
                                     have_mcvs1, have_mcvs2, inner_rel);
            } else {
                // Handle reversed join arguments
                Oid commop = get_commutator(operator);
                Oid commopfuncoid = OidIsValid(commop) ? get_opcode(commop) : InvalidOid;

                selec = eqjoinsel_semi(commopfuncoid, collation, &vardata2, &vardata1,
                                     nd2, nd1, isdefault2, isdefault1,
                                     &sslot2, &sslot1, stats2, stats1,
                                     have_mcvs2, have_mcvs1, inner_rel);
            }

            // Ensure semi-join selectivity doesn't exceed inner-join selectivity
            selec = Min(selec, inner_rel->rows * selec_inner);
            break;

        default:
            elog(ERROR, "unrecognized join type: %d", (int) sjinfo->jointype);
            selec = 0;
            break;
    }

    // Clean up allocated memory
    free_attstatsslot(&sslot1);
    free_attstatsslot(&sslot2);
    ReleaseVariableStats(vardata1);
    ReleaseVariableStats(vardata2);

    // Ensure result is a valid probability
    CLAMP_PROBABILITY(selec);

    PG_RETURN_FLOAT8((float8) selec);
}
```