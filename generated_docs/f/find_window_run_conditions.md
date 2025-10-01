# find_window_run_conditions

## Location
[src/backend/optimizer/path/allpaths.c:2214-2406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L2214-L2406)

## Overview
Analyzes window functions to determine if their monotonic properties can be used to create run conditions that short-circuit window execution based on comparison operators.

## Definition
```c
static bool find_window_run_conditions(Query *subquery, RangeTblEntry *rte, Index rti,
                                       AttrNumber attno, WindowFunc *wfunc, OpExpr *opexpr,
                                       bool wfunc_left, bool *keep_original,
                                       Bitmapset **run_cond_attrs)
```

## Detailed Description
This function implements an optimization for window functions by detecting cases where monotonic window functions (like row_number()) can use run conditions to terminate execution early. For example, if row_number() is used in a subquery with an outer WHERE clause filtering rows <= 10, the window operation can stop once it reaches row 11. The function calls the window function's support function to determine monotonic properties (increasing, decreasing, both, or neither), then matches appropriate comparison operators to create WindowFuncRunCondition nodes that enable early termination. It handles various comparison strategies (=, <, <=, >, >=) and determines whether the original filter condition should be kept or can be replaced entirely by the run condition.

## Parameters / Member Variables
- `subquery`: Query containing the window function
- `rte`: RangeTblEntry for the relation containing the window function
- `rti`: Range table index
- `attno`: Attribute number of the window function column
- `wfunc`: The WindowFunc node to analyze
- `opexpr`: OpExpr node representing the comparison condition
- `wfunc_left`: Boolean indicating if window function is on left side of comparison
- `keep_original`: Output parameter indicating if original condition should be preserved
- `run_cond_attrs`: Bitmapset tracking attributes used in run conditions

## Dependencies
- Functions called/Symbols referenced:
  - IsA (type checking macro)
  - [contain_subplans](../c/contain_subplans.md) (subplan detection)
  - [get_func_support](../g/get_func_support.md) (retrieve support function OID)
  - [is_pseudo_constant_clause](../i/is_pseudo_constant_clause.md) (check for constant expressions)
  - [list_nth](../l/list_nth.md) (list access)
  - OidFunctionCall1 (call support function)
  - [get_op_btree_interpretation](../g/get_op_btree_interpretation.md) (operator analysis)
  - [get_opfamily_member](../g/get_opfamily_member.md) (operator family lookup)
  - makeNode (node creation)
  - copyObject (node copying)
  - [lappend](../l/lappend.md) (list append)
  - [bms_add_member](../b/bms_add_member.md) (bitmapset manipulation)
- Called from (representative examples):
  - [check_and_push_window_quals](../c/check_and_push_window_quals.md)

## Notes and Other Information
- This is a static function accessible only within allpaths.c
- Works with monotonic function types: MONOTONICFUNC_INCREASING, MONOTONICFUNC_DECREASING, MONOTONICFUNC_BOTH, MONOTONICFUNC_NONE
- Handles RelabelType wrapping around WindowFunc nodes
- Rejects window functions containing subplans for simplicity
- Requires the comparison value to be pseudo-constant (unchanging within partition)
- Supports btree strategy numbers for comparison operators (BTLessStrategyNumber, BTEqualStrategyNumber, etc.)
- Creates WindowFuncRunCondition nodes that are attached to the WindowFunc
- Located in src/backend/optimizer/path/allpaths.c at lines 2214-2406
- Critical optimization for queries with window functions and filtering conditions

## Simplified Source
```c
static bool
find_window_run_conditions(Query *subquery, RangeTblEntry *rte, Index rti,
                          AttrNumber attno, WindowFunc *wfunc, OpExpr *opexpr,
                          bool wfunc_left, bool *keep_original,
                          Bitmapset **run_cond_attrs)
{
    Oid prosupport;
    Expr *otherexpr;
    SupportRequestWFuncMonotonic req;
    SupportRequestWFuncMonotonic *res;
    WindowClause *wclause;
    OpExpr *runopexpr = NULL;
    Oid runoperator = InvalidOid;

    *keep_original = true;

    // Strip RelabelType wrapper if present
    while (IsA(wfunc, RelabelType))
        wfunc = (WindowFunc *) ((RelabelType *) wfunc)->arg;

    // Must be a WindowFunc
    if (!IsA(wfunc, WindowFunc))
        return false;

    // Skip if contains subplans (complexity)
    if (contain_subplans((Node *) wfunc))
        return false;

    // Get support function
    prosupport = get_func_support(wfunc->winfnoid);
    if (!OidIsValid(prosupport))
        return false;

    // Get the comparison value (must be constant)
    otherexpr = wfunc_left ? lsecond(opexpr->args) : linitial(opexpr->args);
    if (!is_pseudo_constant_clause((Node *) otherexpr))
        return false;

    // Find window clause and call support function
    wclause = (WindowClause *) list_nth(subquery->windowClause, wfunc->winref - 1);
    req.type = T_SupportRequestWFuncMonotonic;
    req.window_func = wfunc;
    req.window_clause = wclause;

    res = (SupportRequestWFuncMonotonic *)
        DatumGetPointer(OidFunctionCall1(prosupport, PointerGetDatum(&req)));

    // Must be monotonic
    if (res == NULL || res->monotonic == MONOTONICFUNC_NONE)
        return false;

    // Find suitable operator based on monotonicity and comparison type
    List *opinfos = get_op_btree_interpretation(opexpr->opno);
    foreach(lc, opinfos) {
        OpBtreeInterpretation *opinfo = (OpBtreeInterpretation *) lfirst(lc);

        // Handle different comparison strategies
        if (can_create_run_condition(opinfo->strategy, res->monotonic, wfunc_left)) {
            *keep_original = determine_keep_original(opinfo->strategy);
            runopexpr = opexpr;
            runoperator = determine_run_operator(opinfo, res->monotonic, wfunc_left);
            break;
        }
    }

    // Create run condition if suitable operator found
    if (runopexpr != NULL) {
        WindowFuncRunCondition *wfuncrc = makeNode(WindowFuncRunCondition);
        wfuncrc->opno = runoperator;
        wfuncrc->inputcollid = runopexpr->inputcollid;
        wfuncrc->wfunc_left = wfunc_left;
        wfuncrc->arg = copyObject(otherexpr);

        wfunc->runCondition = lappend(wfunc->runCondition, wfuncrc);
        *run_cond_attrs = bms_add_member(*run_cond_attrs,
                                        attno - FirstLowInvalidHeapAttributeNumber);
        return true;
    }

    return false;
}
```