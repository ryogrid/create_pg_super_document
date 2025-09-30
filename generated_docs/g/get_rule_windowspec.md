# get_rule_windowspec

## Location
[src/backend/utils/adt/ruleutils.c:6538-6646](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L6538-L6646)

## Overview
Formats and outputs a complete window specification definition for SQL rule deparsing, including partition clauses, ordering, and frame specifications.

## Definition
```c
static void get_rule_windowspec(WindowClause *wc, List *targetList,
                                deparse_context *context)
```

## Detailed Description
This function generates the textual representation of a window specification, which defines how window functions partition and order data. It handles all components of window specifications:

- **Reference names**: For windows that inherit from named window definitions
- **PARTITION BY clauses**: Column expressions for partitioning data
- **ORDER BY clauses**: Sort specifications within partitions  
- **Frame clauses**: Row/range/group frame boundaries with various options

The function intelligently handles inheritance - partition clauses are always inherited from referenced windows, so they're only printed when no reference name exists. Order clauses are only printed if not inherited (copiedOrder=false). Frame clauses are never inherited and are printed unless they match the default settings.

Frame clause generation supports:
- Frame types: ROWS, RANGE, GROUPS
- Boundary types: UNBOUNDED PRECEDING/FOLLOWING, CURRENT ROW, offset expressions
- BETWEEN syntax for explicit start/end boundaries  
- EXCLUDE options: CURRENT ROW, GROUP, TIES

## Parameters / Member Variables
- `wc`: WindowClause structure containing the complete window specification
- `targetList`: Target list for resolving column references in PARTITION BY and ORDER BY
- `context`: Deparse context with output buffer and formatting state

## Dependencies
- Functions called/Symbols referenced:
  - [quote_identifier](../q/quote_identifier.md) (for proper quoting of reference window names)
  - [get_rule_sortgroupclause](get_rule_sortgroupclause.md) (for PARTITION BY column expressions)
  - [get_rule_orderby](get_rule_orderby.md) (for ORDER BY clause formatting)
  - [get_rule_expr](get_rule_expr.md) (for frame boundary offset expressions)
- Called from (representative examples):
  - [get_rule_windowclause](get_rule_windowclause.md) (for named window definitions)
  - [get_windowfunc_expr_helper](get_windowfunc_expr_helper.md) (for inline window specifications)

## Notes and Other Information
- Static function accessible only within ruleutils.c
- Handles complex frame option bitmask logic with multiple FRAMEOPTION constants
- Optimizes output by avoiding redundant clauses based on inheritance rules
- Located at src/backend/utils/adt/ruleutils.c:6538-6646
- Critical for accurate reconstruction of window function calls in views and rules

## Simplified Source

```c
static void get_rule_windowspec(WindowClause *wc, List *targetList,
                                deparse_context *context) {
    StringInfo buf = context->buf;
    bool needspace = false;

    appendStringInfoChar(buf, '(');

    // Add reference window name if specified
    if (wc->refname) {
        appendStringInfoString(buf, quote_identifier(wc->refname));
        needspace = true;
    }

    // PARTITION BY clause (only if no reference, since it's always inherited)
    if (wc->partitionClause && !wc->refname) {
        if (needspace) appendStringInfoChar(buf, ' ');
        appendStringInfoString(buf, "PARTITION BY ");

        const char *sep = "";
        foreach(l, wc->partitionClause) {
            SortGroupClause *grp = (SortGroupClause *) lfirst(l);
            appendStringInfoString(buf, sep);
            get_rule_sortgroupclause(grp->tleSortGroupRef, targetList, false, context);
            sep = ", ";
        }
        needspace = true;
    }

    // ORDER BY clause (only if not inherited)
    if (wc->orderClause && !wc->copiedOrder) {
        if (needspace) appendStringInfoChar(buf, ' ');
        appendStringInfoString(buf, "ORDER BY ");
        get_rule_orderby(wc->orderClause, targetList, false, context);
        needspace = true;
    }

    // Frame clause (never inherited, only if non-default)
    if (wc->frameOptions & FRAMEOPTION_NONDEFAULT) {
        if (needspace) appendStringInfoChar(buf, ' ');

        // Frame type: RANGE, ROWS, or GROUPS
        if (wc->frameOptions & FRAMEOPTION_RANGE)
            appendStringInfoString(buf, "RANGE ");
        else if (wc->frameOptions & FRAMEOPTION_ROWS)
            appendStringInfoString(buf, "ROWS ");
        else if (wc->frameOptions & FRAMEOPTION_GROUPS)
            appendStringInfoString(buf, "GROUPS ");

        // BETWEEN syntax for explicit boundaries
        if (wc->frameOptions & FRAMEOPTION_BETWEEN)
            appendStringInfoString(buf, "BETWEEN ");

        // Start boundary
        if (wc->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING)
            appendStringInfoString(buf, "UNBOUNDED PRECEDING ");
        else if (wc->frameOptions & FRAMEOPTION_START_CURRENT_ROW)
            appendStringInfoString(buf, "CURRENT ROW ");
        else if (wc->frameOptions & FRAMEOPTION_START_OFFSET) {
            get_rule_expr(wc->startOffset, context, false);
            if (wc->frameOptions & FRAMEOPTION_START_OFFSET_PRECEDING)
                appendStringInfoString(buf, " PRECEDING ");
            else if (wc->frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING)
                appendStringInfoString(buf, " FOLLOWING ");
        }

        // End boundary for BETWEEN syntax
        if (wc->frameOptions & FRAMEOPTION_BETWEEN) {
            appendStringInfoString(buf, "AND ");
            if (wc->frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING)
                appendStringInfoString(buf, "UNBOUNDED FOLLOWING ");
            else if (wc->frameOptions & FRAMEOPTION_END_CURRENT_ROW)
                appendStringInfoString(buf, "CURRENT ROW ");
            else if (wc->frameOptions & FRAMEOPTION_END_OFFSET) {
                get_rule_expr(wc->endOffset, context, false);
                if (wc->frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING)
                    appendStringInfoString(buf, " PRECEDING ");
                else if (wc->frameOptions & FRAMEOPTION_END_OFFSET_FOLLOWING)
                    appendStringInfoString(buf, " FOLLOWING ");
            }
        }

        // EXCLUDE options
        if (wc->frameOptions & FRAMEOPTION_EXCLUDE_CURRENT_ROW)
            appendStringInfoString(buf, "EXCLUDE CURRENT ROW ");
        else if (wc->frameOptions & FRAMEOPTION_EXCLUDE_GROUP)
            appendStringInfoString(buf, "EXCLUDE GROUP ");
        else if (wc->frameOptions & FRAMEOPTION_EXCLUDE_TIES)
            appendStringInfoString(buf, "EXCLUDE TIES ");

        // Remove trailing space
        buf->len--;
    }

    appendStringInfoChar(buf, ')');
}
```