# statext_is_compatible_clause_internal

## Location
[src/backend/statistics/extended_stats.c:1331-1557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L1331-L1557)

## Overview
Recursively determines if a clause is compatible with MCV (Most Common Values) lists by analyzing the clause structure and extracting supported sub-expressions and variables.

## Definition

```c
static bool
statext_is_compatible_clause_internal(PlannerInfo *root, Node *clause,
									  Index relid, Bitmapset **attnums,
									  List **exprs, bool *leakproof)
```
## Detailed Description
This internal function recursively examines SQL clauses to determine compatibility with extended statistics MCV lists. It supports a specific set of clause types including OpExprs with comparison operators (=, <, >, >=, <=), NULL tests, ScalarArrayOpExprs (IN/ANY/ALL), and Boolean combinations (AND/OR/NOT). The function extracts variable attribute numbers and sub-expressions that need to be matched against statistics objects. It also tracks the leakproofness of operators to ensure security constraints are maintained during statistics-based estimation.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planning context information
- `*clause`: Node representing the (sub)clause to be inspected (bare clause, not RestrictInfo)
- `relid`: Relation index that all variables in the clause must belong to
- `**attnums`: Input/output bitmap collecting attribute numbers of mentioned variables
- `**exprs`: Input/output list collecting primitive subclauses within the clause tree
- `*leakproof`: Input/output flag tracking leakproofness of the clause tree (starts true, set false if non-leakproof operators found)
## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - AttrNumberIsForUserDefinedAttr
  - [bms_add_member](../b/bms_add_member.md)
  - [is_opclause](../i/is_opclause.md)
  - [examine_opclause_args](../e/examine_opclause_args.md)
  - [get_oprrest](../g/get_oprrest.md)
  - [get_opcode](../g/get_opcode.md)
  - [get_func_leakproof](../g/get_func_leakproof.md)
  - [is_andclause](../i/is_andclause.md)
  - [is_orclause](../i/is_orclause.md)
  - [is_notclause](../i/is_notclause.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [statext_is_compatible_clause_internal](statext_is_compatible_clause_internal.md) (recursive calls)
  - [statext_is_compatible_clause](statext_is_compatible_clause.md)

## Notes and Other Information
The function uses recursive descent parsing to handle nested clause structures. It rejects system attributes and whole-row variables since statistics cannot be collected on them. For operator expressions, it validates that operators use supported selectivity estimation functions (F_EQSEL, F_NEQSEL, etc.). The leakproof tracking ensures that security-sensitive queries maintain their security properties when using extended statistics. Future expansions may support more complex cases like Var op Var comparisons.

## Simplified Source

```c
static bool
statext_is_compatible_clause_internal(PlannerInfo *root, Node *clause,
                                      Index relid, Bitmapset **attnums,
                                      List **exprs, bool *leakproof)
{
    // Handle binary-compatible relabeling
    if (IsA(clause, RelabelType))
        clause = (Node *) ((RelabelType *) clause)->arg;

    // Plain Var references (boolean Vars or recursive checks)
    if (IsA(clause, Var)) {
        Var *var = (Var *) clause;

        // Validate var belongs to correct relation and level
        if (var->varno != relid || var->varlevelsup > 0)
            return false;

        // Reject system attributes and whole-row Vars
        if (!AttrNumberIsForUserDefinedAttr(var->varattno))
            return false;

        // Add attribute number for later permissions checks
        *attnums = bms_add_member(*attnums, var->varattno);
        return true;
    }

    // (Var/Expr op Const) or (Const op Var/Expr)
    if (is_opclause(clause)) {
        OpExpr *expr = (OpExpr *) clause;
        Node *clause_expr;

        // Only support two-argument expressions
        if (list_length(expr->args) != 2)
            return false;

        if (!examine_opclause_args(expr->args, &clause_expr, NULL, NULL))
            return false;

        // Check if operator is supported (=, <, >, <=, >=, !=)
        switch (get_oprrest(expr->opno)) {
            case F_EQSEL: case F_NEQSEL: case F_SCALARLTSEL:
            case F_SCALARLESEL: case F_SCALARGTSEL: case F_SCALARGESEL:
                break;  // Supported operators
            default:
                return false;  // Unsupported operator
        }

        // Track leakproofness
        if (*leakproof)
            *leakproof = get_func_leakproof(get_opcode(expr->opno));

        // Recursively check Var expressions or add to expressions list
        if (IsA(clause_expr, Var))
            return statext_is_compatible_clause_internal(root, clause_expr,
                                                        relid, attnums,
                                                        exprs, leakproof);
        *exprs = lappend(*exprs, clause_expr);
        return true;
    }

    // Var/Expr IN Array (ScalarArrayOpExpr)
    if (IsA(clause, ScalarArrayOpExpr)) {
        ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) clause;
        Node *clause_expr;
        bool expronleft;

        if (list_length(expr->args) != 2)
            return false;

        if (!examine_opclause_args(expr->args, &clause_expr, NULL, &expronleft))
            return false;

        // Only support Var on left, Array on right
        if (!expronleft)
            return false;

        // Validate operator support (same as OpExpr)
        // ... similar operator validation as above

        // Recursively process Var or add expression
        if (IsA(clause_expr, Var))
            return statext_is_compatible_clause_internal(root, clause_expr,
                                                        relid, attnums,
                                                        exprs, leakproof);
        *exprs = lappend(*exprs, clause_expr);
        return true;
    }

    // AND/OR/NOT clauses - recursively check all subclauses
    if (is_andclause(clause) || is_orclause(clause) || is_notclause(clause)) {
        BoolExpr *expr = (BoolExpr *) clause;
        ListCell *lc;

        foreach(lc, expr->args) {
            if (!statext_is_compatible_clause_internal(root, (Node *) lfirst(lc),
                                                      relid, attnums, exprs,
                                                      leakproof))
                return false;
        }
        return true;
    }

    // Var/Expr IS NULL tests
    if (IsA(clause, NullTest)) {
        NullTest *nt = (NullTest *) clause;

        if (IsA(nt->arg, Var))
            return statext_is_compatible_clause_internal(root, (Node *) nt->arg,
                                                        relid, attnums,
                                                        exprs, leakproof);
        *exprs = lappend(*exprs, nt->arg);
        return true;
    }

    // Treat other expressions as bare expressions
    *exprs = lappend(*exprs, clause);
    return true;
}
```