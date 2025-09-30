# transformWindowDefinitions

## Location
[src/backend/parser/parse_clause.c:2765-2984](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L2765-L2984)

## Overview
Transforms window definitions (WindowDef nodes) into WindowClause nodes, handling window references, partition/order clauses, and frame specifications according to SQL standard rules.

## Definition

```c
struction ensures we follow the rule
	 * that sortClause and distinctClause match;
```
## Detailed Description
This function processes WINDOW clause definitions and inline window specifications from SQL queries. For each WindowDef in the input list, it creates a corresponding WindowClause node with transformed partition and order clauses. The function handles complex window inheritance rules from SQL:2008 standard: referenced windows copy partition clauses (which cannot be overridden), may copy order clauses (only if the referenced window has none), and cannot copy frame clauses (referenced windows with frame clauses cause errors). It validates window name uniqueness, resolves window references, transforms PARTITION BY clauses using transformGroupClause, and transforms ORDER BY clauses using transformSortClause. Special handling is provided for RANGE frame mode with offsets (requires exactly one ORDER BY column) and GROUPS frame mode (requires an ORDER BY clause). Frame offset expressions are processed through transformFrameOffset.

## Parameters / Member Variables
- : ParseState containing parsing context and state information
- : List of WindowDef nodes to be transformed from the SQL WINDOW clause
- : Reference to TargetEntry list where partition/order expressions are added as resjunk

## Dependencies
- Functions called/Symbols referenced:
  - [findWindowClause](../f/findWindowClause.md)
  - [transformSortClause](transformSortClause.md)
  - [transformGroupClause](transformGroupClause.md)
  - [transformFrameOffset](transformFrameOffset.md)
  - copyObject
  - makeNode
  - [get_sortgroupclause_expr](../g/get_sortgroupclause_expr.md)
  - [get_ordering_op_properties](../g/get_ordering_op_properties.md)
  - [exprCollation](../e/exprCollation.md)
  - linitial_node
  - [WindowDef](../W/WindowDef.md), WindowClause, SortGroupClause (struct types)
  - EXPR_KIND_WINDOW_ORDER, EXPR_KIND_WINDOW_PARTITION (enum values)
  - FRAMEOPTION_DEFAULTS, FRAMEOPTION_RANGE, FRAMEOPTION_GROUPS, FRAMEOPTION_START_OFFSET, FRAMEOPTION_END_OFFSET (frame option constants)
  - BTLessStrategyNumber (B-tree strategy constant)
- Called from (representative examples):
  - [transformSelectStmt](transformSelectStmt.md)
  - [transformPLAssignStmt](transformPLAssignStmt.md)

## Notes and Other Information
- This is a public function declared in parse_clause.h
- Implements SQL:2008 window clause inheritance rules with their complex and somewhat bizarre semantics
- Always forces SQL99 rules for partition and order clause interpretation regardless of server settings
- Enforces strict validation for window references and prevents circular references
- RANGE mode with offsets requires exactly one ORDER BY column and determines sort operator properties
- GROUPS mode mandates the presence of an ORDER BY clause
- Window reference numbers (winref) are assigned sequentially for query execution
- Frame clause inheritance is prohibited by SQL standard, leading to specific error messages
- Part of PostgreSQL's comprehensive window function support implementing SQL standard windowing

## Simplified Source

```c
List *
transformWindowDefinitions(ParseState *pstate,
                           List *windowdefs,
                           List **targetlist)
{
    List *result = NIL;
    Index winref = 0;
    ListCell *lc;

    foreach(lc, windowdefs) {
        WindowDef *windef = (WindowDef *) lfirst(lc);
        WindowClause *refwc = NULL;
        List *partitionClause;
        List *orderClause;
        Oid rangeopfamily = InvalidOid;
        Oid rangeopcintype = InvalidOid;
        WindowClause *wc;

        winref++;

        // Check for duplicate window names
        if (windef->name &&
            findWindowClause(result, windef->name) != NULL)
            ereport(ERROR, /* window already defined */);

        // Look up referenced window if any
        if (windef->refname) {
            refwc = findWindowClause(result, windef->refname);
            if (refwc == NULL)
                ereport(ERROR, /* window does not exist */);
        }

        // Transform PARTITION and ORDER specs (similar to GROUP BY and ORDER BY)
        orderClause = transformSortClause(pstate,
                                          windef->orderClause,
                                          targetlist,
                                          EXPR_KIND_WINDOW_ORDER,
                                          true /* force SQL99 rules */);
        partitionClause = transformGroupClause(pstate,
                                               windef->partitionClause,
                                               NULL,
                                               targetlist,
                                               orderClause,
                                               EXPR_KIND_WINDOW_PARTITION,
                                               true /* force SQL99 rules */);

        // Create new WindowClause
        wc = makeNode(WindowClause);
        wc->name = windef->name;
        wc->refname = windef->refname;

        // Handle window reference inheritance rules per SQL:2008
        if (refwc) {
            if (partitionClause)
                ereport(ERROR, /* cannot override PARTITION BY clause */);
            wc->partitionClause = copyObject(refwc->partitionClause);
        }
        else
            wc->partitionClause = partitionClause;

        if (refwc) {
            if (orderClause && refwc->orderClause)
                ereport(ERROR, /* cannot override ORDER BY clause */);
            if (orderClause) {
                wc->orderClause = orderClause;
                wc->copiedOrder = false;
            }
            else {
                wc->orderClause = copyObject(refwc->orderClause);
                wc->copiedOrder = true;
            }
        }
        else {
            wc->orderClause = orderClause;
            wc->copiedOrder = false;
        }

        // Check frame clause inheritance rules
        if (refwc && refwc->frameOptions != FRAMEOPTION_DEFAULTS) {
            if (windef->name ||
                orderClause || windef->frameOptions != FRAMEOPTION_DEFAULTS)
                ereport(ERROR, /* cannot copy window with frame clause */);
            else
                ereport(ERROR, /* cannot copy window with frame clause (hint: omit parentheses) */);
        }

        wc->frameOptions = windef->frameOptions;

        // RANGE offset requires exactly one ORDER BY column
        if ((wc->frameOptions & FRAMEOPTION_RANGE) &&
            (wc->frameOptions & (FRAMEOPTION_START_OFFSET |
                                 FRAMEOPTION_END_OFFSET))) {
            SortGroupClause *sortcl;
            Node *sortkey;
            int16 rangestrategy;

            if (list_length(wc->orderClause) != 1)
                ereport(ERROR, /* RANGE with offset requires exactly one ORDER BY column */);

            sortcl = linitial_node(SortGroupClause, wc->orderClause);
            sortkey = get_sortgroupclause_expr(sortcl, *targetlist);

            if (!get_ordering_op_properties(sortcl->sortop,
                                            &rangeopfamily,
                                            &rangeopcintype,
                                            &rangestrategy))
                elog(ERROR, "operator %u is not a valid ordering operator",
                     sortcl->sortop);

            // Record properties of sort ordering
            wc->inRangeColl = exprCollation(sortkey);
            wc->inRangeAsc = (rangestrategy == BTLessStrategyNumber);
            wc->inRangeNullsFirst = sortcl->nulls_first;
        }

        // GROUPS mode requires an ORDER BY clause
        if (wc->frameOptions & FRAMEOPTION_GROUPS) {
            if (wc->orderClause == NIL)
                ereport(ERROR, /* GROUPS mode requires an ORDER BY clause */);
        }

        // Process frame offset expressions
        wc->startOffset = transformFrameOffset(pstate, wc->frameOptions,
                                               rangeopfamily, rangeopcintype,
                                               &wc->startInRangeFunc,
                                               windef->startOffset);
        wc->endOffset = transformFrameOffset(pstate, wc->frameOptions,
                                             rangeopfamily, rangeopcintype,
                                             &wc->endInRangeFunc,
                                             windef->endOffset);
        wc->winref = winref;

        result = lappend(result, wc);
    }

    return result;
}
```