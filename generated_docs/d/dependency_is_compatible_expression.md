# dependency_is_compatible_expression

## Location
[src/backend/statistics/dependencies.c:1168-1369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L1168-L1369)

## Overview
Determines if an expression is compatible with functional dependencies by checking if it matches a statistics expression in the provided statistics list, extending beyond simple Var expressions to support complex expressions.

## Definition

```c
static bool
dependency_is_compatible_expression(Node *clause, Index relid, List *statlist, Node **expr)
```
## Detailed Description
This function serves as an extended version of  that supports complex expressions beyond simple Var nodes. It evaluates whether a clause can be used with functional dependencies by checking if the clause's expression matches any of the expressions tracked in extended statistics.

The function handles the same clause types as :
- **OpExpr**:  or  patterns
- **ScalarArrayOpExpr**:  with ANY semantics
- **OR clauses**: Recursively processes all sub-clauses ensuring they reference the same expression
- **NOT clauses**: Interprets  as 
- **Boolean expressions**: Interprets bare boolean  as 

The key difference is that instead of requiring a simple Var, this function searches through the provided statistics list to find a matching expression. This enables functional dependency usage with computed expressions like , , etc., that have extended statistics collected on them.

## Parameters / Member Variables
- `*clause`: The clause node to examine for compatibility
- `relid`: The relation index that the clause should reference
- `*statlist`: List of StatisticExtInfo structures containing tracked expressions
- `**expr`: Output parameter that receives the matching statistics expression on success
## Dependencies
- Functions called/Symbols referenced:
  - [bms_membership](../b/bms_membership.md)
  - [is_opclause](../i/is_opclause.md)
  - [is_pseudo_constant_clause](../i/is_pseudo_constant_clause.md)
  - [get_oprrest](../g/get_oprrest.md)
  - [is_orclause](../i/is_orclause.md)
  - [is_notclause](../i/is_notclause.md)
  - [get_notclausearg](../g/get_notclausearg.md)
  - [equal](../e/equal.md) (for expression comparison)
- Types used:
  - [StatisticExtInfo](../S/StatisticExtInfo.md)
  - STATS_EXT_DEPENDENCIES
- Called from (representative examples):
  - DependencyGenerator
  - [dependency_is_compatible_expression](dependency_is_compatible_expression.md) (recursive call for OR clauses)
  - [dependencies_clauselist_selectivity](dependencies_clauselist_selectivity.md)

## Notes and Other Information
- Extends compatibility checking beyond simple Var expressions to complex expressions tracked in extended statistics
- Maintains the same clause validation logic as
- Uses expression equality () to match clause expressions with statistics expressions
- Requires that expressions be tracked in extended statistics with dependency information
- For OR clauses, ensures all sub-expressions are identical using  comparison
- Enables functional dependency optimization for computed columns and expression indexes
- The function is recursive when processing OR clauses to ensure all sub-expressions match the same statistics expression

## Simplified Source

```c
static bool
dependency_is_compatible_expression(Node *clause, Index relid, List *statlist, Node **expr)
{
    ListCell *lc, *lc2;
    Node *clause_expr;

    // Handle RestrictInfo wrapper
    if (IsA(clause, RestrictInfo)) {
        RestrictInfo *rinfo = (RestrictInfo *) clause;

        if (rinfo->pseudoconstant)
            return false;
        if (bms_membership(rinfo->clause_relids) != BMS_SINGLETON)
            return false;

        clause = (Node *) rinfo->clause;
    }

    // Handle different clause types
    if (is_opclause(clause)) {
        // Check for Var = Const or Const = Var pattern
        OpExpr *expr_op = (OpExpr *) clause;

        if (list_length(expr_op->args) != 2)
            return false;

        // Extract non-constant expression
        if (is_pseudo_constant_clause(lsecond(expr_op->args)))
            clause_expr = linitial(expr_op->args);
        else if (is_pseudo_constant_clause(linitial(expr_op->args)))
            clause_expr = lsecond(expr_op->args);
        else
            return false;

        // Only equality operators are supported
        if (get_oprrest(expr_op->opno) != F_EQSEL)
            return false;
    }
    else if (IsA(clause, ScalarArrayOpExpr)) {
        // Check for Var IN Const pattern
        ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;

        if (!saop->useOr || list_length(saop->args) != 2)
            return false;
        if (!is_pseudo_constant_clause(lsecond(saop->args)))
            return false;
        if (get_oprrest(saop->opno) != F_EQSEL)
            return false;

        clause_expr = linitial(saop->args);
    }
    else if (is_orclause(clause)) {
        // Handle OR clauses recursively
        BoolExpr *bool_expr = (BoolExpr *) clause;
        *expr = NULL;

        foreach(lc, bool_expr->args) {
            Node *or_expr = NULL;

            if (!dependency_is_compatible_expression((Node *) lfirst(lc), relid, statlist, &or_expr))
                return false;

            if (*expr == NULL)
                *expr = or_expr;
            if (!equal(or_expr, *expr))
                return false;
        }
        return true;
    }
    else if (is_notclause(clause)) {
        // NOT x treated as x = false
        clause_expr = (Node *) get_notclausearg(clause);
    }
    else {
        // Boolean x treated as x = true
        clause_expr = (Node *) clause;
    }

    // Strip RelabelType wrapper
    if (IsA(clause_expr, RelabelType))
        clause_expr = (Node *) ((RelabelType *) clause_expr)->arg;

    // Search for matching expression in statistics
    foreach(lc, statlist) {
        StatisticExtInfo *info = (StatisticExtInfo *) lfirst(lc);

        if (info->kind != STATS_EXT_DEPENDENCIES)
            continue;

        foreach(lc2, info->exprs) {
            Node *stat_expr = (Node *) lfirst(lc2);

            if (equal(clause_expr, stat_expr)) {
                *expr = stat_expr;
                return true;
            }
        }
    }

    return false;
}
```