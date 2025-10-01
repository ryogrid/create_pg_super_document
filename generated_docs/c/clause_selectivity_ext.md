# clause_selectivity_ext

## Location
[src/backend/optimizer/path/clausesel.c:684-973](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/clausesel.c#L684-L973)

## Overview
Extended version of clause_selectivity that provides fine-grained control over extended statistics usage and implements the core logic for computing selectivity of general boolean expression clauses.

## Definition
```c
Selectivity
clause_selectivity_ext(PlannerInfo *root,
                       Node *clause,
                       int varRelid,
                       JoinType jointype,
                       SpecialJoinInfo *sjinfo,
                       bool use_extended_stats)
```

## Detailed Description
This function serves as the comprehensive selectivity estimation engine for PostgreSQL's query optimizer. It handles a wide variety of clause types including variables, constants, parameters, logical operations (NOT, AND, OR), operator expressions, function calls, scalar array operations, row comparisons, null tests, boolean tests, and type coercion expressions.

Key features include:
1. **Caching mechanism**: For RestrictInfo clauses, results are cached in norm_selec (JOIN_INNER) or outer_selec (other join types) fields to avoid redundant calculations
2. **Extended statistics control**: The use_extended_stats parameter allows selective enabling/disabling of extended statistics for fine-tuned estimation
3. **Pseudoconstant handling**: Pseudoconstant clauses return 1.0 selectivity (except FALSE constants which return 0.0)
4. **Recursive processing**: Handles complex nested expressions by recursively calling itself for subexpressions
5. **Join vs. restriction classification**: Uses treat_as_join_clause to determine appropriate selectivity estimation method

The function implements sophisticated logic for different expression types:
- **Variables**: Uses boolvarsel for boolean variables
- **Constants**: Returns exact selectivity (0.0 for FALSE/NULL, 1.0 for TRUE)
- **Logical operations**: Implements proper AND/OR logic and NOT inversion
- **Operator clauses**: Delegates to join_selectivity or restriction_selectivity based on clause classification
- **Function calls**: Uses function-specific selectivity estimation
- **Special constructs**: Handles array operations, row comparisons, null tests, and boolean tests

## Parameters / Member Variables
- `root`: PlannerInfo structure containing optimizer state and statistics
- `clause`: Node representing the boolean expression (RestrictInfo or plain expression)
- `varRelid`: Relation ID for restriction mode (0 for join mode)
- `jointype`: Type of join operation affecting selectivity calculation
- `sjinfo`: SpecialJoinInfo providing join context information
- `use_extended_stats`: Flag controlling whether to use extended statistics

## Dependencies
- Functions called/Symbols referenced:
  - [treat_as_join_clause](../t/treat_as_join_clause.md) (for clause classification)
  - [clauselist_selectivity_ext](clauselist_selectivity_ext.md) (for AND clauses)
  - [clauselist_selectivity_or](clauselist_selectivity_or.md) (for OR clauses) 
  - [join_selectivity](../j/join_selectivity.md) (for join clauses)
  - [restriction_selectivity](../r/restriction_selectivity.md) (for restriction clauses)
  - [function_selectivity](../f/function_selectivity.md) (for function expressions)
  - [boolvarsel](../b/boolvarsel.md) (for boolean variables)
  - [scalararraysel](../s/scalararraysel.md), rowcomparesel, nulltestsel, booltestsel (for specific node types)
  - [estimate_expression_value](../e/estimate_expression_value.md) (for parameter evaluation)
- Called from (representative examples):
  - [clause_selectivity](clause_selectivity.md) (standard interface)
  - [clauselist_selectivity_ext](clauselist_selectivity_ext.md) (for recursive AND processing)
  - [clauselist_selectivity_or](clauselist_selectivity_or.md) (for recursive OR processing)
  - [statext_mcv_clauselist_selectivity](../s/statext_mcv_clauselist_selectivity.md) (extended statistics)

## Notes and Other Information
- Default selectivity is 0.5 for unhandled clause types
- Implements sophisticated caching strategy based on varRelid and join context
- Supports debugging through SELECTIVITY_DEBUG compilation flag
- Handles type coercion transparently (RelabelType, CoerceToDomain)
- Uses different cache fields for INNER vs. outer joins to handle examination with different join types
- Contains extensive comments explaining caching conditions and join type variations
- The function is central to PostgreSQL's cost-based optimization and affects query plan selection significantly

## Simplified Source

```c
Selectivity clause_selectivity_ext(PlannerInfo *root, Node *clause, int varRelid,
                                  JoinType jointype, SpecialJoinInfo *sjinfo,
                                  bool use_extended_stats) {
    Selectivity s1 = 0.5;  // Default selectivity
    RestrictInfo *rinfo = NULL;
    bool cacheable = false;

    if (clause == NULL)
        return s1;

    // Handle RestrictInfo wrapper
    if (IsA(clause, RestrictInfo)) {
        rinfo = (RestrictInfo *) clause;

        // Pseudoconstant clauses return 1.0 (except FALSE)
        if (rinfo->pseudoconstant) {
            if (!IsA(rinfo->clause, Const))
                return 1.0;
        }

        // Check if result can be cached
        if (varRelid == 0 || rinfo->num_base_rels == 0 ||
            (rinfo->num_base_rels == 1 &&
             bms_is_member(varRelid, rinfo->clause_relids))) {

            // Return cached result if available
            if (jointype == JOIN_INNER && rinfo->norm_selec >= 0)
                return rinfo->norm_selec;
            else if (jointype != JOIN_INNER && rinfo->outer_selec >= 0)
                return rinfo->outer_selec;

            cacheable = true;
        }

        // Extract the actual clause
        clause = rinfo->orclause ? (Node *) rinfo->orclause : (Node *) rinfo->clause;
    }

    // Process different clause types
    if (IsA(clause, Var)) {
        Var *var = (Var *) clause;
        if (var->varlevelsup == 0 &&
            (varRelid == 0 || varRelid == (int) var->varno)) {
            s1 = boolvarsel(root, (Node *) var, varRelid);
        }
    }
    else if (IsA(clause, Const)) {
        Const *con = (Const *) clause;
        s1 = con->constisnull ? 0.0 :
             DatumGetBool(con->constvalue) ? 1.0 : 0.0;
    }
    else if (IsA(clause, Param)) {
        Node *subst = estimate_expression_value(root, clause);
        if (IsA(subst, Const)) {
            Const *con = (Const *) subst;
            s1 = con->constisnull ? 0.0 :
                 DatumGetBool(con->constvalue) ? 1.0 : 0.0;
        }
    }
    else if (is_notclause(clause)) {
        // NOT clause: invert selectivity
        s1 = 1.0 - clause_selectivity_ext(root,
                                         (Node *) get_notclausearg((Expr *) clause),
                                         varRelid, jointype, sjinfo, use_extended_stats);
    }
    else if (is_andclause(clause)) {
        // AND clause: use clauselist_selectivity
        s1 = clauselist_selectivity_ext(root, ((BoolExpr *) clause)->args,
                                       varRelid, jointype, sjinfo, use_extended_stats);
    }
    else if (is_orclause(clause)) {
        // OR clause: use OR-specific logic
        s1 = clauselist_selectivity_or(root, ((BoolExpr *) clause)->args,
                                      varRelid, jointype, sjinfo, use_extended_stats);
    }
    else if (is_opclause(clause) || IsA(clause, DistinctExpr)) {
        OpExpr *opclause = (OpExpr *) clause;

        if (treat_as_join_clause(root, clause, rinfo, varRelid, sjinfo)) {
            s1 = join_selectivity(root, opclause->opno, opclause->args,
                                 opclause->inputcollid, jointype, sjinfo);
        } else {
            s1 = restriction_selectivity(root, opclause->opno, opclause->args,
                                        opclause->inputcollid, varRelid);
        }

        // DistinctExpr: negate the result
        if (IsA(clause, DistinctExpr))
            s1 = 1.0 - s1;
    }
    else if (is_funcclause(clause)) {
        FuncExpr *funcclause = (FuncExpr *) clause;
        s1 = function_selectivity(root, funcclause->funcid, funcclause->args,
                                 funcclause->inputcollid,
                                 treat_as_join_clause(root, clause, rinfo, varRelid, sjinfo),
                                 varRelid, jointype, sjinfo);
    }
    else if (IsA(clause, ScalarArrayOpExpr)) {
        s1 = scalararraysel(root, (ScalarArrayOpExpr *) clause,
                           treat_as_join_clause(root, clause, rinfo, varRelid, sjinfo),
                           varRelid, jointype, sjinfo);
    }
    else if (IsA(clause, RowCompareExpr)) {
        s1 = rowcomparesel(root, (RowCompareExpr *) clause, varRelid, jointype, sjinfo);
    }
    else if (IsA(clause, NullTest)) {
        NullTest *nulltest = (NullTest *) clause;
        s1 = nulltestsel(root, nulltest->nulltesttype, (Node *) nulltest->arg,
                        varRelid, jointype, sjinfo);
    }
    else if (IsA(clause, BooleanTest)) {
        BooleanTest *booltest = (BooleanTest *) clause;
        s1 = booltestsel(root, booltest->booltesttype, (Node *) booltest->arg,
                        varRelid, jointype, sjinfo);
    }
    else if (IsA(clause, CurrentOfExpr)) {
        CurrentOfExpr *cexpr = (CurrentOfExpr *) clause;
        RelOptInfo *crel = find_base_rel(root, cexpr->cvarno);
        if (crel->tuples > 0)
            s1 = 1.0 / crel->tuples;
    }
    else if (IsA(clause, RelabelType)) {
        s1 = clause_selectivity_ext(root, (Node *) ((RelabelType *) clause)->arg,
                                   varRelid, jointype, sjinfo, use_extended_stats);
    }
    else if (IsA(clause, CoerceToDomain)) {
        s1 = clause_selectivity_ext(root, (Node *) ((CoerceToDomain *) clause)->arg,
                                   varRelid, jointype, sjinfo, use_extended_stats);
    }
    else {
        // Default: treat as boolean variable
        s1 = boolvarsel(root, clause, varRelid);
    }

    // Cache the result if possible
    if (cacheable) {
        if (jointype == JOIN_INNER)
            rinfo->norm_selec = s1;
        else
            rinfo->outer_selec = s1;
    }

    return s1;
}
```