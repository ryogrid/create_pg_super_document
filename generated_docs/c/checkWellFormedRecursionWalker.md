# checkWellFormedRecursionWalker

## Location
[src/backend/parser/parse_cte.c:1027-1206](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_cte.c#L1027-L1206)

## Overview
checkWellFormedRecursionWalker is a recursive tree walker function that traverses SQL parse trees to detect and validate self-references in recursive CTE queries, ensuring they appear only in valid contexts and with proper frequency.

## Definition
```c
static bool checkWellFormedRecursionWalker(Node *node, CteState *cstate)
```

## Detailed Description
This function implements a specialized tree walker that enforces PostgreSQL's rules for recursive CTE self-references. It performs context-aware validation by:

**Self-Reference Detection:**
- Identifies RangeVar nodes that reference the current recursive CTE being validated
- Checks inner WITH clause scope to ensure references aren't captured by nested CTEs
- Counts self-references to ensure exactly one appears in the recursive term

**Context-Sensitive Validation:**
- **RECURSION_OK**: Valid context where self-references are allowed
- **RECURSION_NONRECURSIVETERM**: Non-recursive term where self-references are forbidden
- **RECURSION_SUBLINK**: Subqueries where self-references are forbidden
- **RECURSION_OUTERJOIN**: Outer join contexts where self-references have restrictions

**Special Node Handling:**
- **SelectStmt**: Handles nested WITH clauses with proper visibility scoping (recursive vs non-recursive)
- **JoinExpr**: Applies context restrictions for different join types (outer joins change context to RECURSION_OUTERJOIN)
- **SubLink**: Changes context to RECURSION_SUBLINK for subquery validation
- **WithClause**: Prevents uncontrolled recursion into nested WITH clauses

**Visibility Management:**
- Maintains innerwiths stack to track CTE visibility at different nesting levels
- Implements different scoping rules for recursive vs non-recursive WITH clauses
- Ensures proper CTE name resolution according to SQL standard semantics

The walker integrates with the generic raw_expression_tree_walker for comprehensive tree traversal while providing specialized handling for recursion-sensitive constructs.

## Parameters / Member Variables
- `node`: The current parse tree node being examined
- `cstate`: CTE validation state containing current item context, recursion counters, inner WITH scope stack, and error reporting information

## Dependencies
- Functions called/Symbols referenced:
  - raw_expression_tree_walker (generic tree traversal)
  - [checkWellFormedSelectStmt](checkWellFormedSelectStmt.md) (SELECT statement validation)
  - ereport (error reporting)
  - [errcode](../e/errcode.md) (error code specification) 
  - [errmsg](../e/errmsg.md) (error message formatting)
  - [parser_errposition](../p/parser_errposition.md) (parse location for errors)
  - strcmp (string comparison)
  - [lcons](../l/lcons.md) (list construction)
  - [lappend](../l/lappend.md) (list append)
  - [list_delete_first](../l/list_delete_first.md) (list manipulation)
  - [list_head](../l/list_head.md) (list access)
  - IsA (type checking macro)
  - elog (internal error logging)
  - [RecursionContext](../R/RecursionContext.md) (recursion context enum)
  - [RangeVar](../R/RangeVar.md) (table reference structure)
  - [SelectStmt](../S/SelectStmt.md) (SELECT statement structure)
  - [JoinExpr](../J/JoinExpr.md) (join expression structure)
  - [SubLink](../S/SubLink.md) (sublink structure)
  - [WithClause](../W/WithClause.md) (WITH clause structure)
  - CommonTableExpr (CTE structure)
  - RECURSION_* constants (context enumeration values)
  - JOIN_* constants (join type enumeration values)

- Called from:
  - [checkWellFormedRecursion](checkWellFormedRecursion.md) (main validation controller)
  - [checkWellFormedSelectStmt](checkWellFormedSelectStmt.md) (SELECT statement processing)
  - Self-recursively for tree traversal

## Notes and Other Information
- The walker implements a context-sensitive state machine for recursion validation
- Different SQL constructs impose different restrictions on where recursive self-references can appear
- The function maintains proper scoping for nested WITH clauses to prevent incorrect reference capture
- Error messages provide specific parse locations and context-appropriate explanations
- The walker handles both recursive and non-recursive WITH clauses with different visibility semantics
- Outer join handling is critical because recursive references in outer join contexts can produce incorrect results
- The function prevents infinite loops by controlling WITH clause recursion and limiting raw_expression_tree_walker usage
- Self-reference counting ensures the recursive term has exactly one self-reference (more or fewer is invalid)

## Simplified Source

```c
static bool
checkWellFormedRecursionWalker(Node *node, CteState *cstate)
{
    RecursionContext save_context = cstate->context;

    if (node == NULL)
        return false;

    // Check for CTE self-references in RangeVar nodes
    if (IsA(node, RangeVar))
    {
        RangeVar *rv = (RangeVar *) node;

        // Skip qualified names (can't be CTEs)
        if (rv->schemaname)
            return false;

        // Check if captured by inner WITH clause
        foreach(lc, cstate->innerwiths)
        {
            List *withlist = (List *) lfirst(lc);
            foreach(lc2, withlist)
            {
                CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc2);
                if (strcmp(rv->relname, cte->ctename) == 0)
                    return false;  // Captured by inner WITH
            }
        }

        // Check if this references the current CTE
        CommonTableExpr *mycte = cstate->items[cstate->curitem].cte;
        if (strcmp(rv->relname, mycte->ctename) == 0)
        {
            // Found recursive reference - validate context
            if (cstate->context != RECURSION_OK)
                ereport(ERROR, (errcode(ERRCODE_INVALID_RECURSION),
                               errmsg(recursion_errormsgs[cstate->context],
                                      mycte->ctename),
                               parser_errposition(cstate->pstate, rv->location)));

            // Ensure only one self-reference
            if (++(cstate->selfrefcount) > 1)
                ereport(ERROR, (errcode(ERRCODE_INVALID_RECURSION),
                               errmsg("recursive reference to query \"%s\" must not appear more than once",
                                      mycte->ctename),
                               parser_errposition(cstate->pstate, rv->location)));
        }
        return false;
    }

    // Handle SELECT statements with WITH clauses
    if (IsA(node, SelectStmt))
    {
        SelectStmt *stmt = (SelectStmt *) node;
        if (stmt->withClause)
        {
            if (stmt->withClause->recursive)
            {
                // Recursive WITH: all CTEs visible to all
                cstate->innerwiths = lcons(stmt->withClause->ctes, cstate->innerwiths);
                foreach(lc, stmt->withClause->ctes)
                    checkWellFormedRecursionWalker(((CommonTableExpr *) lfirst(lc))->ctequery, cstate);
                checkWellFormedSelectStmt(stmt, cstate);
                cstate->innerwiths = list_delete_first(cstate->innerwiths);
            }
            else
            {
                // Non-recursive WITH: sequential visibility
                cstate->innerwiths = lcons(NIL, cstate->innerwiths);
                foreach(lc, stmt->withClause->ctes)
                {
                    CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
                    checkWellFormedRecursionWalker(cte->ctequery, cstate);
                    ListCell *cell1 = list_head(cstate->innerwiths);
                    lfirst(cell1) = lappend((List *) lfirst(cell1), cte);
                }
                checkWellFormedSelectStmt(stmt, cstate);
                cstate->innerwiths = list_delete_first(cstate->innerwiths);
            }
        }
        else
            checkWellFormedSelectStmt(stmt, cstate);
        return false;
    }

    // Handle join expressions with context changes
    if (IsA(node, JoinExpr))
    {
        JoinExpr *j = (JoinExpr *) node;
        switch (j->jointype)
        {
            case JOIN_INNER:
                checkWellFormedRecursionWalker(j->larg, cstate);
                checkWellFormedRecursionWalker(j->rarg, cstate);
                checkWellFormedRecursionWalker(j->quals, cstate);
                break;
            case JOIN_LEFT:
                checkWellFormedRecursionWalker(j->larg, cstate);
                if (save_context == RECURSION_OK)
                    cstate->context = RECURSION_OUTERJOIN;
                checkWellFormedRecursionWalker(j->rarg, cstate);
                cstate->context = save_context;
                checkWellFormedRecursionWalker(j->quals, cstate);
                break;
            case JOIN_RIGHT:
                if (save_context == RECURSION_OK)
                    cstate->context = RECURSION_OUTERJOIN;
                checkWellFormedRecursionWalker(j->larg, cstate);
                cstate->context = save_context;
                checkWellFormedRecursionWalker(j->rarg, cstate);
                checkWellFormedRecursionWalker(j->quals, cstate);
                break;
            case JOIN_FULL:
                if (save_context == RECURSION_OK)
                    cstate->context = RECURSION_OUTERJOIN;
                checkWellFormedRecursionWalker(j->larg, cstate);
                checkWellFormedRecursionWalker(j->rarg, cstate);
                cstate->context = save_context;
                checkWellFormedRecursionWalker(j->quals, cstate);
                break;
        }
        return false;
    }

    // Handle sublinks with context change
    if (IsA(node, SubLink))
    {
        SubLink *sl = (SubLink *) node;
        cstate->context = RECURSION_SUBLINK;
        checkWellFormedRecursionWalker(sl->subselect, cstate);
        cstate->context = save_context;
        checkWellFormedRecursionWalker(sl->testexpr, cstate);
        return false;
    }

    // Prevent direct WITH clause recursion
    if (IsA(node, WithClause))
        return false;

    // Continue with generic tree walker
    return raw_expression_tree_walker(node, checkWellFormedRecursionWalker, (void *) cstate);
}
```