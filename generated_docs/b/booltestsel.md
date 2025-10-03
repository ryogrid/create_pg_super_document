# booltestsel

## Location
[src/backend/utils/adt/selfuncs.c:1541-1698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L1541-L1698)

## Overview
Computes the selectivity of BooleanTest nodes, handling SQL Boolean test operations like IS TRUE, IS FALSE, IS UNKNOWN and their negated forms.

## Definition

```c
Selectivity
booltestsel(PlannerInfo *root, BoolTestType booltesttype, Node *arg,
			int varRelid, JoinType jointype, SpecialJoinInfo *sjinfo)
```
## Detailed Description
The  function estimates selectivity for Boolean test expressions in SQL queries, such as , , , etc. It implements sophisticated logic to handle the three-valued Boolean logic of SQL (TRUE, FALSE, NULL/UNKNOWN).

The function operates in three tiers of sophistication:

1. **Full Statistics Available**: When both most-common-values (MCV) and null fraction statistics are available, it calculates precise frequencies for TRUE, FALSE, and NULL values, then applies the appropriate Boolean test logic.

2. **Partial Statistics**: When only null fraction data is available, it uses that for IS [NOT] UNKNOWN tests and assumes a 50-50 split between TRUE and FALSE for non-NULL values.

3. **No Statistics**: Falls back to using  on the underlying expression with default selectivity constants for UNKNOWN tests.

The function handles all six Boolean test types: IS TRUE, IS NOT TRUE, IS FALSE, IS NOT FALSE, IS UNKNOWN, and IS NOT UNKNOWN, each with specific selectivity calculation logic that respects SQL's three-valued logic.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and context information
- `booltesttype`: Type of Boolean test (IS_TRUE, IS_FALSE, IS_UNKNOWN, IS_NOT_TRUE, IS_NOT_FALSE, IS_NOT_UNKNOWN)
- `*arg`: Node representing the expression being tested
- `varRelid`: Relation ID to restrict analysis to (0 if no restriction)
- `jointype`: Type of join operation context
- `*sjinfo`: Special join information for outer joins
## Dependencies
- Functions called/Symbols referenced:
  - [examine_variable](../e/examine_variable.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - [clause_selectivity](../c/clause_selectivity.md)
  - ReleaseVariableStats
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)
  - [GenericCosts](../G/GenericCosts.md)

## Notes and Other Information
- Handles SQL's three-valued Boolean logic (TRUE, FALSE, NULL/UNKNOWN) correctly
- Uses sophisticated statistical analysis when MCV (most-common-values) data is available
- Falls back gracefully through multiple levels of statistical data availability
- Ensures result is within valid probability range using CLAMP_PROBABILITY
- Critical for accurate selectivity estimation of Boolean test operations in query optimization
- Supports all six Boolean test operations defined in SQL standard

## Simplified Source

```c
Selectivity
booltestsel(PlannerInfo *root, BoolTestType booltesttype, Node *arg,
            int varRelid, JoinType jointype, SpecialJoinInfo *sjinfo)
{
    VariableStatData vardata;
    double selec;

    examine_variable(root, arg, varRelid, &vardata);

    if (HeapTupleIsValid(vardata.statsTuple)) {
        // Statistics available - calculate precise frequencies
        Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(vardata.statsTuple);
        double freq_null = stats->stanullfrac;

        // Try to get Most Common Values statistics
        AttStatsSlot sslot;
        if (get_attstatsslot(&sslot, vardata.statsTuple, STATISTIC_KIND_MCV,
                            InvalidOid, ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS)
            && sslot.nnumbers > 0) {

            // Calculate frequencies for TRUE and FALSE values
            double freq_true, freq_false;
            if (DatumGetBool(sslot.values[0])) {
                freq_true = sslot.numbers[0];
            } else {
                freq_true = 1.0 - sslot.numbers[0] - freq_null;
            }
            freq_false = 1.0 - freq_true - freq_null;

            // Apply Boolean test logic
            switch (booltesttype) {
                case IS_UNKNOWN:     selec = freq_null; break;
                case IS_NOT_UNKNOWN: selec = 1.0 - freq_null; break;
                case IS_TRUE:        selec = freq_true; break;
                case IS_NOT_TRUE:    selec = 1.0 - freq_true; break;
                case IS_FALSE:       selec = freq_false; break;
                case IS_NOT_FALSE:   selec = 1.0 - freq_false; break;
                default:
                    elog(ERROR, "unrecognized booltesttype: %d", (int) booltesttype);
                    selec = 0.0;
            }
            free_attstatsslot(&sslot);
        } else {
            // Only null fraction available - assume 50/50 split for non-nulls
            switch (booltesttype) {
                case IS_UNKNOWN:     selec = freq_null; break;
                case IS_NOT_UNKNOWN: selec = 1.0 - freq_null; break;
                case IS_TRUE:
                case IS_FALSE:       selec = (1.0 - freq_null) / 2.0; break;
                case IS_NOT_TRUE:
                case IS_NOT_FALSE:   selec = (freq_null + 1.0) / 2.0; break;
                default:
                    elog(ERROR, "unrecognized booltesttype: %d", (int) booltesttype);
                    selec = 0.0;
            }
        }
    } else {
        // No statistics - use defaults or clause selectivity
        switch (booltesttype) {
            case IS_UNKNOWN:     selec = DEFAULT_UNK_SEL; break;
            case IS_NOT_UNKNOWN: selec = DEFAULT_NOT_UNK_SEL; break;
            case IS_TRUE:
            case IS_NOT_FALSE:
                selec = (double) clause_selectivity(root, arg, varRelid, jointype, sjinfo);
                break;
            case IS_FALSE:
            case IS_NOT_TRUE:
                selec = 1.0 - (double) clause_selectivity(root, arg, varRelid, jointype, sjinfo);
                break;
            default:
                elog(ERROR, "unrecognized booltesttype: %d", (int) booltesttype);
                selec = 0.0;
        }
    }

    ReleaseVariableStats(vardata);
    CLAMP_PROBABILITY(selec);
    return (Selectivity) selec;
}
```