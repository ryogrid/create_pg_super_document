# negate_clause

## Location
[src/backend/optimizer/prep/prepqual.c:73-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepqual.c#L73-L292)

## Overview
Negates a Boolean expression by logical simplification, attempting to eliminate the NOT node through DeMorgan's laws and other boolean transformations rather than simply wrapping the expression in a NOT clause.

## Definition

```c
structure;
```
## Detailed Description
The  function takes a Boolean expression and returns its logical negation, but does so intelligently by applying various logical simplification rules rather than just adding a NOT node. It is primarily designed as a helper function for  and preserves AND/OR flat structure in the input.

Key transformations applied:
- **Constants**: Negates boolean constants directly (true becomes false, false becomes true, NULL remains NULL)
- **Operators**: Uses negator operators when available (< becomes >=, = becomes <>, etc.)
- **ScalarArrayOpExpr**: Negates array operators and flips ANY/ALL semantics
- **BoolExpr**: Applies DeMorgan's laws to AND/OR expressions:
  - NOT(A AND B) becomes (NOT A) OR (NOT B)
  - NOT(A OR B) becomes (NOT A) AND (NOT B)
  - NOT(NOT A) becomes A (double negation elimination)
- **NullTest**: Flips IS NULL to IS NOT NULL and vice versa (scalar types only)
- **BooleanTest**: Flips various boolean test types (IS_TRUE to IS_NOT_TRUE, etc.)

The function unconditionally applies DeMorgan's laws even if it results in more NOT nodes, because exposing top-level AND/OR structure is crucial for WHERE clause optimization and ensuring logically equivalent expressions are physically equal.

## Parameters / Member Variables
- : The Boolean expression node to negate (should not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  -  - determines the node type
  -  - creates boolean constant nodes
  -  - finds the negator operator for a given operator
  -  - creates OR expression nodes
  -  - creates AND expression nodes
  -  - creates NOT expression nodes as fallback
- Called from (representative examples):
  -  (src/backend/optimizer/util/clauses.c:2905)
  -  (src/backend/optimizer/util/clauses.c:4006)
  -  (src/backend/partitioning/partprune.c:3736)
  - Recursively calls itself when processing AND/OR expressions

## Notes and Other Information
- The function preserves the AND/OR flat property of input expressions, which is important for query optimization
- For expressions that cannot be simplified, it falls back to wrapping with an explicit NOT node
- The transformation ensures that logically equivalent expressions will be physically equal after processing
- Handles special cases like double negation elimination and null handling appropriately
- Part of the PostgreSQL query optimizer's constant expression evaluation and boolean simplification system

## Simplified Source

```c
Node *negate_clause(Node *node) {
    if (node == NULL)
        elog(ERROR, "can't negate an empty subexpression");

    switch (nodeTag(node)) {
        case T_Const: {
            Const *c = (Const *) node;
            // NOT NULL is still NULL
            if (c->constisnull)
                return makeBoolConst(false, true);
            // Negate boolean constant
            return makeBoolConst(!DatumGetBool(c->constvalue), false);
        }

        case T_OpExpr: {
            // Use negator operator if available (e.g., < becomes >=)
            OpExpr *opexpr = (OpExpr *) node;
            Oid negator = get_negator(opexpr->opno);
            if (negator) {
                OpExpr *newopexpr = makeNode(OpExpr);
                newopexpr->opno = negator;
                newopexpr->opfuncid = InvalidOid;
                newopexpr->opresulttype = opexpr->opresulttype;
                newopexpr->opretset = opexpr->opretset;
                newopexpr->opcollid = opexpr->opcollid;
                newopexpr->inputcollid = opexpr->inputcollid;
                newopexpr->args = opexpr->args;
                newopexpr->location = opexpr->location;
                return (Node *) newopexpr;
            }
            break;
        }

        case T_BoolExpr: {
            BoolExpr *expr = (BoolExpr *) node;
            switch (expr->boolop) {
                case AND_EXPR: {
                    // Apply DeMorgan's law: NOT(A AND B) => (NOT A) OR (NOT B)
                    List *nargs = NIL;
                    foreach(lc, expr->args) {
                        nargs = lappend(nargs, negate_clause(lfirst(lc)));
                    }
                    return (Node *) make_orclause(nargs);
                }
                case OR_EXPR: {
                    // Apply DeMorgan's law: NOT(A OR B) => (NOT A) AND (NOT B)
                    List *nargs = NIL;
                    foreach(lc, expr->args) {
                        nargs = lappend(nargs, negate_clause(lfirst(lc)));
                    }
                    return (Node *) make_andclause(nargs);
                }
                case NOT_EXPR:
                    // Double negation elimination: NOT(NOT A) => A
                    return (Node *) linitial(expr->args);
            }
            break;
        }

        case T_NullTest: {
            // Flip IS NULL <-> IS NOT NULL (scalar types only)
            NullTest *expr = (NullTest *) node;
            if (!expr->argisrow) {
                NullTest *newexpr = makeNode(NullTest);
                newexpr->arg = expr->arg;
                newexpr->nulltesttype = (expr->nulltesttype == IS_NULL ?
                                        IS_NOT_NULL : IS_NULL);
                newexpr->argisrow = expr->argisrow;
                newexpr->location = expr->location;
                return (Node *) newexpr;
            }
            break;
        }

        case T_BooleanTest: {
            // Flip boolean test types (IS_TRUE <-> IS_NOT_TRUE, etc.)
            BooleanTest *expr = (BooleanTest *) node;
            BooleanTest *newexpr = makeNode(BooleanTest);
            newexpr->arg = expr->arg;
            // Map each test type to its negation
            switch (expr->booltesttype) {
                case IS_TRUE: newexpr->booltesttype = IS_NOT_TRUE; break;
                case IS_NOT_TRUE: newexpr->booltesttype = IS_TRUE; break;
                case IS_FALSE: newexpr->booltesttype = IS_NOT_FALSE; break;
                case IS_NOT_FALSE: newexpr->booltesttype = IS_FALSE; break;
                case IS_UNKNOWN: newexpr->booltesttype = IS_NOT_UNKNOWN; break;
                case IS_NOT_UNKNOWN: newexpr->booltesttype = IS_UNKNOWN; break;
            }
            newexpr->location = expr->location;
            return (Node *) newexpr;
        }
    }

    // Fallback: wrap with explicit NOT node
    return (Node *) make_notclause((Expr *) node);
}
```