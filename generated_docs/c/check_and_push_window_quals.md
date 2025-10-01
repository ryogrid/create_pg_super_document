# check_and_push_window_quals

## Location
[src/backend/optimizer/path/allpaths.c:2407-2481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L2407-L2481)

## Overview
Analyzes a WHERE clause condition to determine if it can be pushed down as a run condition into WindowFunc nodes for early termination optimization.

## Definition
```c
static bool check_and_push_window_quals(Query *subquery, RangeTblEntry *rte, Index rti,
                                        Node *clause, Bitmapset **run_cond_attrs)
```

## Detailed Description
This function serves as the entry point for window function run condition optimization. It examines a qualification clause (typically from a WHERE condition) to see if it references window functions that can benefit from run conditions. The function validates that the clause is a suitable OpExpr with exactly two operands and uses a strict operator function. It then checks both operands of the OpExpr to find Var nodes that reference window functions in the subquery's target list. When such references are found, it calls find_window_run_conditions() to attempt to create run conditions that can terminate window processing early. The optimization is particularly valuable for queries where window functions like row_number() are filtered with comparison operators in outer queries.

## Parameters / Member Variables
- `subquery`: Query containing the window functions to potentially optimize
- `rte`: RangeTblEntry for the relation being processed
- `rti`: Range table index for the relation
- `clause`: Node representing the qualification clause to analyze (expected to be OpExpr)
- `run_cond_attrs`: Bitmapset collecting attribute numbers that receive run conditions

## Dependencies
- Functions called/Symbols referenced:
  - IsA (type checking macro)
  - [list_length](../l/list_length.md) (list utility function)
  - [set_opfuncid](../s/set_opfuncid.md) (set operator function ID)
  - [func_strict](../f/func_strict.md) (check if function is strict)
  - linitial (get first list element)
  - lsecond (get second list element)
  - [list_nth](../l/list_nth.md) (get nth list element)
  - [find_window_run_conditions](../f/find_window_run_conditions.md) (analyze specific window function)
  - [OpExpr](../O/OpExpr.md) (struct type)
  - [Var](../V/Var.md) (struct type)
  - [TargetEntry](../T/TargetEntry.md) (struct type)
  - [WindowFunc](../W/WindowFunc.md) (struct type)
- Called from (representative examples):
  - [set_subquery_pathlist](../s/set_subquery_pathlist.md)

## Notes and Other Information
- This is a static function accessible only within allpaths.c
- Restricts optimization to strict OpExprs to ensure proper NULL handling when window functions are terminated early
- Returns true if the original clause must be kept, false if it can be safely replaced by the run condition
- Checks both left and right operands of the OpExpr for window function references
- Requires exactly 2 operands in the OpExpr for processing
- The run condition optimization sets window function results to NULL when termination occurs, relying on strict operators to filter these appropriately
- Located in src/backend/optimizer/path/allpaths.c at lines 2407-2481
- Part of the broader window function optimization framework in PostgreSQL

## Simplified Source

```c
static bool
check_and_push_window_quals(Query *subquery, RangeTblEntry *rte, Index rti,
                           Node *clause, Bitmapset **run_cond_attrs)
{
    OpExpr *opexpr = (OpExpr *) clause;
    bool keep_original = true;
    Var *var1, *var2;

    // Only process OpExprs with exactly 2 operands
    if (!IsA(opexpr, OpExpr) || list_length(opexpr->args) != 2)
        return true;

    // Require strict operators for proper NULL handling
    set_opfuncid(opexpr);
    if (!func_strict(opexpr->opfuncid))
        return true;

    // Check left operand for window function reference
    var1 = linitial(opexpr->args);
    if (IsA(var1, Var) && var1->varattno > 0)
    {
        TargetEntry *tle = list_nth(subquery->targetList, var1->varattno - 1);
        WindowFunc *wfunc = (WindowFunc *) tle->expr;

        if (find_window_run_conditions(subquery, rte, rti, tle->resno, wfunc,
                                     opexpr, true, &keep_original,
                                     run_cond_attrs))
            return keep_original;
    }

    // Check right operand for window function reference
    var2 = lsecond(opexpr->args);
    if (IsA(var2, Var) && var2->varattno > 0)
    {
        TargetEntry *tle = list_nth(subquery->targetList, var2->varattno - 1);
        WindowFunc *wfunc = (WindowFunc *) tle->expr;

        if (find_window_run_conditions(subquery, rte, rti, tle->resno, wfunc,
                                     opexpr, false, &keep_original,
                                     run_cond_attrs))
            return keep_original;
    }

    return true;
}
```