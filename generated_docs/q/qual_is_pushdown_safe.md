# qual_is_pushdown_safe

## Location
[src/backend/optimizer/path/allpaths.c:3855-3955](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L3855-L3955)

## Overview
This function determines whether a specific restriction clause (qualifier) can be safely pushed down into a subquery during query optimization, checking various safety conditions to prevent semantic errors.

## Definition
```c
static pushdown_safe_type qual_is_pushdown_safe(Query *subquery, Index rti, RestrictInfo *rinfo, pushdown_safety_info *safetyInfo)
```

## Detailed Description
This function performs comprehensive safety analysis for qualifier pushdown optimization. It examines a restriction clause that applies to a given subquery and determines if pushing the clause into the subquery would preserve the query's semantics and correctness.

The function implements five key safety checks:

1. **SubPlan Detection**: Rejects clauses containing SubPlans, as these may not work correctly when pushed down (SubLinks transformed to SubPlans in the outer qual but not in the subquery). Note that SubLinks that become initplans are safe because they appear as Param references.

2. **Volatile Function Safety**: When unsafeVolatile flag is set, rejects clauses containing volatile functions that could produce different results when executed at different query levels.

3. **Leaky Function Safety**: When unsafeLeaky flag is set, rejects clauses with functions that might leak subquery values as side effects when passed Var nodes.

4. **Whole-row Reference Check**: Rejects clauses referencing the subquery's whole-row output (varattno == 0), as there's no easy way to reference this within the subquery itself.

5. **Column Safety Check**: Verifies that all referenced subquery output columns were deemed safe by prior analysis in subquery_is_pushdown_safe().

The function can return different safety levels: completely safe, unsafe, or safe only as window clause run conditions.

## Parameters / Member Variables
- `subquery`: The subquery that the restriction clause might be pushed into
- `rti`: The range table index of the subquery in the parent query
- `rinfo`: The RestrictInfo containing the restriction clause to be analyzed
- `safetyInfo`: Structure containing safety flags and information about column-level safety

## Dependencies
- Functions called/Symbols referenced:
  - [contain_subplans](../c/contain_subplans.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [contain_leaked_vars](../c/contain_leaked_vars.md)
  - [pull_var_clause](../p/pull_var_clause.md)
  - lfirst
  - IsA
  - Assert
  - [list_free](../l/list_free.md)
- Types/Constants referenced:
  - pushdown_safe_type
  - [pushdown_safety_info](../p/pushdown_safety_info.md)
  - PUSHDOWN_SAFE
  - PUSHDOWN_UNSAFE
  - PUSHDOWN_WINDOWCLAUSE_RUNCOND
  - PVC_INCLUDE_PLACEHOLDERS
  - UNSAFE_HAS_VOLATILE_FUNC
  - UNSAFE_HAS_SET_FUNC
  - UNSAFE_NOTIN_DISTINCTON_CLAUSE
  - UNSAFE_TYPE_MISMATCH
- Called from (representative examples):
  - [set_subquery_pathlist](../s/set_subquery_pathlist.md) (src/backend/optimizer/path/allpaths.c:2568)

## Notes and Other Information
- Static function within allpaths.c, core component of PostgreSQL's qualifier pushdown optimization
- Includes a cheap sanity check that no aggregates or window functions appear in quals (which would be unsafe)
- Punts on PlaceHolderVars due to unclear safety implications and rarity in practice
- Does not handle lateral references (would require converting to outer references)
- UNSAFE_NOTIN_PARTITIONBY_CLAUSE is specifically allowed for window clause run conditions
- Located in src/backend/optimizer/path/allpaths.c:3855-3955

## Simplified Source

```c
static pushdown_safe_type
qual_is_pushdown_safe(Query *subquery, Index rti, RestrictInfo *rinfo,
                     pushdown_safety_info *safetyInfo)
{
    pushdown_safe_type safe = PUSHDOWN_SAFE;
    Node *qual = (Node *) rinfo->clause;
    List *vars;
    ListCell *vl;

    // Reject clauses with SubPlans
    if (contain_subplans(qual))
        return PUSHDOWN_UNSAFE;

    // Reject volatile functions if marked unsafe
    if (safetyInfo->unsafeVolatile &&
        contain_volatile_functions((Node *) rinfo))
        return PUSHDOWN_UNSAFE;

    // Reject leaky functions if marked unsafe
    if (safetyInfo->unsafeLeaky &&
        contain_leaked_vars(qual))
        return PUSHDOWN_UNSAFE;

    // Check all Vars in the clause
    vars = pull_var_clause(qual, PVC_INCLUDE_PLACEHOLDERS);
    foreach(vl, vars)
    {
        Var *var = (Var *) lfirst(vl);

        // Reject PlaceHolderVars
        if (!IsA(var, Var))
        {
            safe = PUSHDOWN_UNSAFE;
            break;
        }

        // Reject lateral references
        if (var->varno != rti)
        {
            safe = PUSHDOWN_UNSAFE;
            break;
        }

        // Reject whole-row references
        if (var->varattno == 0)
        {
            safe = PUSHDOWN_UNSAFE;
            break;
        }

        // Check column-specific safety flags
        if (safetyInfo->unsafeFlags[var->varattno] != 0)
        {
            if (safetyInfo->unsafeFlags[var->varattno] &
                (UNSAFE_HAS_VOLATILE_FUNC | UNSAFE_HAS_SET_FUNC |
                 UNSAFE_NOTIN_DISTINCTON_CLAUSE | UNSAFE_TYPE_MISMATCH))
            {
                safe = PUSHDOWN_UNSAFE;
                break;
            }
            else
            {
                // UNSAFE_NOTIN_PARTITIONBY_CLAUSE allows window run conditions
                safe = PUSHDOWN_WINDOWCLAUSE_RUNCOND;
            }
        }
    }

    list_free(vars);
    return safe;
}
```