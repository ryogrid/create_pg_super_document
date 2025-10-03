# convert_EXISTS_to_ANY

## Location
[src/backend/optimizer/plan/subselect.c:1628-1867](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L1628-L1867)

## Overview
Transforms an EXISTS subquery into a hashable ANY subquery by extracting equality conditions from the WHERE clause, enabling more efficient hash-based execution instead of nested loops.

## Definition

```c
static Query *
convert_EXISTS_to_ANY(PlannerInfo *root, Query *subselect,
					  Node **testexpr, List **paramIds)
```
## Detailed Description
This function attempts to convert an EXISTS subquery into an ANY subquery with hashable conditions, which can be executed more efficiently using hash tables. The transformation works by analyzing the WHERE clause of the EXISTS subquery to find equality conditions between outer and inner query variables.

The conversion process:
1. Extracts and analyzes WHERE clause conditions
2. Identifies equality operators that can be hashed (using )
3. Separates conditions with outer variables from those with only inner variables
4. Creates a new target list for the subquery that outputs the right-hand side values
5. Builds a test expression for the parent query using Params to reference subquery outputs
6. Constructs the equivalent ANY operation with hash-joinable conditions

The function includes extensive validation to ensure the transformation is safe and beneficial, including checks for volatile functions, variable level constraints, and aggregate functions.

## Parameters / Member Variables
- : PlannerInfo structure containing the planning context
- : The EXISTS subquery to be converted (must be a fresh copy and pre-simplified)
- : Output parameter for the test expression to be used in the parent query
- : Output parameter for the list of Param IDs created for the subquery outputs

## Dependencies
- Functions called/Symbols referenced:
  - : Checks for variable references at specific query nesting levels
  - : Detects volatile function calls that prevent optimization
  - : Simplifies constant expressions in WHERE clauses
  - : Canonicalizes qualification expressions
  - : Converts explicit AND operations to implicit form
  - : Determines if an operator can be used for hashing
  - : Finds the commutator operator for proper operand ordering
  - : Creates execution parameters for subquery outputs
  - : Constructs operator clause expressions
  - : Checks for aggregate functions at specific levels
  - : Detects subplan references that prevent optimization
  - : Adjusts variable sublevel references
- Called from (representative examples):
  - : Uses this function to attempt EXISTS-to-ANY conversion before creating subplans

## Notes and Other Information
- Returns the modified subquery on success, NULL on failure
- Requires the input subquery to have already been processed by 
- The function creates Params directly rather than going through 
- Only handles equality conditions with hashable operators - other conditions remain in the subquery
- Performs extensive validation to prevent incorrect transformations
- The conversion enables hash-based ANY execution which can be significantly faster than EXISTS execution for large datasets
- Part of PostgreSQL's subquery optimization framework that transforms correlated subqueries for better performance

## Simplified Source

```c
static Query *
convert_EXISTS_to_ANY(PlannerInfo *root, Query *subselect,
                      Node **testexpr, List **paramIds)
{
    Node       *whereClause;
    List       *leftargs, *rightargs, *opids, *opcollations, *newWhere;
    List       *tlist, *testlist, *paramids;
    AttrNumber  resno;

    // Query must not require a targetlist since we insert a new one
    Assert(subselect->targetList == NIL);

    // Extract WHERE clause from subquery
    whereClause = subselect->jointree->quals;
    subselect->jointree->quals = NULL;

    // Safety checks: no parent vars, no volatile functions
    if (contain_vars_of_level((Node *) subselect, 1) ||
        contain_volatile_functions(whereClause))
        return NULL;

    // Simplify WHERE clause for easier processing
    whereClause = eval_const_expressions(root, whereClause);
    whereClause = (Node *) canonicalize_qual((Expr *) whereClause, false);
    whereClause = (Node *) make_ands_implicit((Expr *) whereClause);

    // Parse WHERE clause to find hashable equality conditions
    leftargs = rightargs = opids = opcollations = newWhere = NIL;
    foreach(lc, (List *) whereClause)
    {
        OpExpr *expr = (OpExpr *) lfirst(lc);

        if (IsA(expr, OpExpr) && hash_ok_operator(expr))
        {
            Node *leftarg = (Node *) linitial(expr->args);
            Node *rightarg = (Node *) lsecond(expr->args);

            // Check if we can extract outer = inner condition
            if (contain_vars_of_level(leftarg, 1))
            {
                leftargs = lappend(leftargs, leftarg);
                rightargs = lappend(rightargs, rightarg);
                opids = lappend_oid(opids, expr->opno);
                opcollations = lappend_oid(opcollations, expr->inputcollid);
                continue;
            }
            // Try commuted version: inner = outer becomes outer = inner
            if (contain_vars_of_level(rightarg, 1))
            {
                expr->opno = get_commutator(expr->opno);
                if (OidIsValid(expr->opno) && hash_ok_operator(expr))
                {
                    leftargs = lappend(leftargs, rightarg);
                    rightargs = lappend(rightargs, leftarg);
                    opids = lappend_oid(opids, expr->opno);
                    opcollations = lappend_oid(opcollations, expr->inputcollid);
                    continue;
                }
                return NULL;  // No commutator available
            }
        }
        // Keep non-hashable conditions in subquery
        newWhere = lappend(newWhere, expr);
    }

    // Must find at least one hashable condition
    if (leftargs == NIL)
        return NULL;

    // Validate variable levels for safety
    if (contain_vars_of_level((Node *) newWhere, 1) ||
        contain_vars_of_level((Node *) rightargs, 1) ||
        contain_vars_of_level((Node *) leftargs, 0))
        return NULL;

    // Additional safety checks for aggregates and subplans
    if (root->parse->hasAggs &&
        (contain_aggs_of_level((Node *) newWhere, 1) ||
         contain_aggs_of_level((Node *) rightargs, 1)))
        return NULL;

    if (contain_subplans((Node *) leftargs))
        return NULL;

    // Adjust variable levels for outer conditions
    IncrementVarSublevelsUp((Node *) leftargs, -1, 1);

    // Put remaining conditions back in subquery
    if (newWhere)
        subselect->jointree->quals = (Node *) make_ands_explicit(newWhere);

    // Build new targetlist and test expressions
    tlist = testlist = paramids = NIL;
    resno = 1;
    forfour(lc, leftargs, rc, rightargs, oc, opids, cc, opcollations)
    {
        Node *leftarg = (Node *) lfirst(lc);
        Node *rightarg = (Node *) lfirst(rc);
        Oid   opid = lfirst_oid(oc);
        Oid   opcollation = lfirst_oid(cc);
        Param *param;

        // Create parameter for subquery output
        param = generate_new_exec_param(root,
                                        exprType(rightarg),
                                        exprTypmod(rightarg),
                                        exprCollation(rightarg));

        // Add to subquery targetlist
        tlist = lappend(tlist,
                        makeTargetEntry((Expr *) rightarg, resno++, NULL, false));

        // Create test condition: leftarg op param
        testlist = lappend(testlist,
                           make_opclause(opid, BOOLOID, false,
                                         (Expr *) leftarg, (Expr *) param,
                                         InvalidOid, opcollation));
        paramids = lappend_int(paramids, param->paramid);
    }

    // Set results and return modified subquery
    subselect->targetList = tlist;
    *testexpr = (Node *) make_ands_explicit(testlist);
    *paramIds = paramids;

    return subselect;
}
```