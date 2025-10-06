# networksel

## Location
[src/backend/utils/adt/network_selfuncs.c:79-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L79-L195)

## Overview
Calculates selectivity estimation for network subnet inclusion/overlap operators, used by the PostgreSQL query planner to estimate how many rows will match network-based WHERE clauses.

## Definition

```c
enum = inet_opr_codenum(operator);
```
## Detailed Description
The  function implements selectivity estimation for PostgreSQL's network data type operators (inet, cidr) when used in WHERE clauses with subnet inclusion and overlap operations. It analyzes column statistics to predict the fraction of rows that will satisfy conditions like  or .

The function follows a systematic approach: first checking if the expression is in the form , then utilizing most-common-values (MCV) statistics if available, and finally applying histogram-based estimation for the remaining population. This dual approach ensures accurate selectivity estimates across different data distributions.

The estimation process combines MCV selectivity (exact matches from frequent values) with histogram-based selectivity for less common values, weighted by their respective population fractions.

## Parameters / Member Variables
- : PlannerInfo pointer containing query planning context
- : OID of the network operator being evaluated  
- : List of arguments to the operator expression
- : Relation ID of the variable, or 0 if not restricted to a relation

## Dependencies
- Functions called/Symbols referenced:
  - [get_restriction_variable](../g/get_restriction_variable.md)
  - [mcv_selectivity](../m/mcv_selectivity.md)
  - [inet_opr_codenum](../i/inet_opr_codenum.md)
  - [inet_hist_value_sel](../i/inet_hist_value_sel.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [get_opcode](../g/get_opcode.md)
  - ReleaseVariableStats
  - DEFAULT_SEL
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - Used as selectivity estimation function registered in system catalogs
  - Invoked by the query planner during optimization

## Notes and Other Information
- Returns default selectivity if the expression is not in the expected  format
- Handles NULL constants by returning 0.0 selectivity (no matches expected)
- Requires column statistics to provide meaningful estimates, falls back to defaults otherwise
- Uses operator-specific histogram analysis through 
- Combines MCV and histogram statistics with proper weighting by population fractions
- Results are clamped to valid probability range [0.0, 1.0]

## Simplified Source

```c
Datum
networksel(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid operator = PG_GETARG_OID(1);
    List *args = (List *) PG_GETARG_POINTER(2);
    int varRelid = PG_GETARG_INT32(3);

    VariableStatData vardata;
    Node *other;
    bool varonleft;
    Selectivity selec, mcv_selec, non_mcv_selec;
    double sumcommon, nullfrac;

    // Check if expression is (variable op constant) or (constant op variable)
    if (!get_restriction_variable(root, args, varRelid, &vardata, &other, &varonleft))
        PG_RETURN_FLOAT8(DEFAULT_SEL(operator));

    // Require constant operand
    if (!IsA(other, Const))
    {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8(DEFAULT_SEL(operator));
    }

    // Handle NULL constants
    if (((Const *) other)->constisnull)
    {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8(0.0);
    }

    Datum constvalue = ((Const *) other)->constvalue;

    // Need statistics for estimation
    if (!HeapTupleIsValid(vardata.statsTuple))
    {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8(DEFAULT_SEL(operator));
    }

    Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(vardata.statsTuple);
    nullfrac = stats->stanullfrac;

    // Calculate selectivity from most-common-values
    FmgrInfo proc;
    fmgr_info(get_opcode(operator), &proc);
    mcv_selec = mcv_selectivity(&vardata, &proc, InvalidOid, constvalue, varonleft, &sumcommon);

    // Use histogram for non-MCV population estimation
    AttStatsSlot hslot;
    if (get_attstatsslot(&hslot, vardata.statsTuple, STATISTIC_KIND_HISTOGRAM, InvalidOid, ATTSTATSSLOT_VALUES))
    {
        int opr_codenum = inet_opr_codenum(operator);
        if (!varonleft)
            opr_codenum = -opr_codenum;

        non_mcv_selec = inet_hist_value_sel(hslot.values, hslot.nvalues, constvalue, opr_codenum);
        free_attstatsslot(&hslot);
    }
    else
    {
        non_mcv_selec = DEFAULT_SEL(operator);
    }

    // Combine MCV and histogram selectivities
    selec = mcv_selec + (1.0 - nullfrac - sumcommon) * non_mcv_selec;
    CLAMP_PROBABILITY(selec);

    ReleaseVariableStats(vardata);
    PG_RETURN_FLOAT8(selec);
}
```