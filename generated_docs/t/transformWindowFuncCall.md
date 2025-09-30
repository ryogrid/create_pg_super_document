# transformWindowFuncCall

## Location
[src/backend/parser/parse_agg.c:820-1077](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L820-L1077)

## Overview
Completes the initial transformation of a window function call after parse_func.c recognizes it as a window function, handling window definition management and validation of window function placement within queries.

## Definition

```c
struct, eg GROUP BY */
				 errmsg("window functions are not allowed in %s",
						ParseExprKindName(pstate->p_expr_kind)),
				 parser_errposition(pstate, wfunc->location)));
```
## Detailed Description
This function performs the final stage of window function transformation by:

1. **Nesting validation**: Ensures window function calls cannot contain other window functions (nested window functions are not allowed)
2. **Context validation**: Validates that the window function appears in an allowed SQL context using a comprehensive switch statement over ParseExprKind values
3. **Window definition management**: Either links the window function to an existing window definition or creates a new one:
   - If the OVER clause specifies a window name, finds the corresponding WINDOW clause
   - Otherwise, attempts to match window properties against existing definitions to avoid duplication
   - Creates a new window definition entry if no match is found
4. **State marking**: Sets the ParseState's p_hasWindowFuncs flag to indicate the presence of window functions

The function enforces SQL standard restrictions by rejecting window functions in inappropriate contexts like WHERE clauses, JOIN conditions, CHECK constraints, and many others.

## Parameters / Member Variables
- : Current parser state containing context information and window definitions list
- : The WindowFunc node being processed, with winref field to be set
- : Window definition specifying partitioning, ordering, and framing clauses

## Dependencies
- Functions called/Symbols referenced:
  - [contain_windowfuncs](../c/contain_windowfuncs.md)
  - [locate_windowfunc](../l/locate_windowfunc.md)
  - [ParseExprKindName](../P/ParseExprKindName.md)
  - [equal](../e/equal.md)
  - [lappend](../l/lappend.md)
  - [list_length](../l/list_length.md)
- Called from (representative examples):
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - [transformJsonAggConstructor](transformJsonAggConstructor.md)

## Notes and Other Information
- Unlike aggregates, only the most closely nested pstate level is considered for window functions
- The function implements comprehensive error reporting with both custom messages and standardized ParseExprKind-based messages
- Window definition deduplication logic matches similar code in optimize_window_clauses
- The extensive switch statement ensures all ParseExprKind values are handled explicitly to catch new additions at compile time

## Simplified Source

```c
void transformWindowFuncCall(ParseState *pstate, WindowFunc *wfunc,
                            WindowDef *windef) {
    // Step 1: Check for nested window functions (not allowed)
    if (pstate->p_hasWindowFuncs && contain_windowfuncs((Node *) wfunc->args)) {
        ereport(ERROR, "window function calls cannot be nested");
    }

    // Step 2: Validate window function placement context
    const char *err = NULL;
    bool errkind = false;

    switch (pstate->p_expr_kind) {
        case EXPR_KIND_SELECT_TARGET:
        case EXPR_KIND_ORDER_BY:
        case EXPR_KIND_DISTINCT_ON:
        case EXPR_KIND_OTHER:
            // These contexts are allowed
            break;

        case EXPR_KIND_WHERE:
        case EXPR_KIND_HAVING:
        case EXPR_KIND_GROUP_BY:
        case EXPR_KIND_FILTER:
            errkind = true;  // Use standard error message
            break;

        case EXPR_KIND_JOIN_ON:
        case EXPR_KIND_JOIN_USING:
            err = "window functions are not allowed in JOIN conditions";
            break;

        default:
            // Most other contexts are not allowed
            err = "window functions are not allowed in this context";
            break;
    }

    if (err) {
        ereport(ERROR, errmsg_internal("%s", err));
    }
    if (errkind) {
        ereport(ERROR, errmsg("window functions are not allowed in %s",
                             ParseExprKindName(pstate->p_expr_kind)));
    }

    // Step 3: Handle window definition - either find existing or create new
    if (windef->name) {
        // Named window: find existing WINDOW clause
        Index winref = 0;
        ListCell *lc;

        foreach(lc, pstate->p_windowdefs) {
            WindowDef *refwin = (WindowDef *) lfirst(lc);
            winref++;

            if (refwin->name && strcmp(refwin->name, windef->name) == 0) {
                wfunc->winref = winref;
                break;
            }
        }

        if (lc == NULL) {
            ereport(ERROR, errmsg("window \"%s\" does not exist", windef->name));
        }
    } else {
        // Anonymous window: try to find matching existing definition
        Index winref = 0;
        ListCell *lc;
        bool found_match = false;

        foreach(lc, pstate->p_windowdefs) {
            WindowDef *refwin = (WindowDef *) lfirst(lc);
            winref++;

            // Check if all window properties match
            if (equal(refwin->partitionClause, windef->partitionClause) &&
                equal(refwin->orderClause, windef->orderClause) &&
                refwin->frameOptions == windef->frameOptions &&
                equal(refwin->startOffset, windef->startOffset) &&
                equal(refwin->endOffset, windef->endOffset)) {
                wfunc->winref = winref;
                found_match = true;
                break;
            }
        }

        if (!found_match) {
            // No matching definition found, create new one
            pstate->p_windowdefs = lappend(pstate->p_windowdefs, windef);
            wfunc->winref = list_length(pstate->p_windowdefs);
        }
    }

    pstate->p_hasWindowFuncs = true;
}
```