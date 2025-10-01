# get_update_query_targetlist_def

## Location
[src/backend/utils/adt/ruleutils.c:6919-7070](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L6919-L7070)

## Overview
Generates the SET clause portion of an UPDATE statement by deparsing the targetList, handling both simple assignments and complex multi-assignment scenarios.

## Definition
```c
static void get_update_query_targetlist_def(Query *query, List *targetList, deparse_context *context, RangeTblEntry *rte)
```

## Detailed Description
This function is responsible for generating the SET clause of UPDATE statements from PostgreSQL's internal targetList representation. It handles complex scenarios including:

1. **Simple assignments**: Standard column = value assignments
2. **Multi-assignments**: Tuple assignments like (col1, col2) = (subquery)
3. **Indirection**: Field access and array subscripts in assignments
4. **MULTIEXPR sublinks**: Subqueries returning multiple columns for tuple assignments

The function processes each TargetEntry in the targetList, filtering out resjunk entries, and formats them as comma-separated assignments. For multi-assignments, it collects MULTIEXPR_SUBLINK entries and groups related columns within parentheses.

Key processing steps:
- Collect MULTIEXPR sublinks for multi-column assignments
- Process each non-junk target entry
- Handle multi-assignment grouping with parentheses
- Resolve column names from system catalogs (not from resname)
- Process field/array indirection
- Generate appropriate assignment expressions

## Parameters / Member Variables
- `query`: The Query structure containing the UPDATE statement being deparsed
- `targetList`: List of TargetEntry nodes representing the SET clause assignments
- `context`: The deparse_context for formatting preferences and output buffer
- `rte`: RangeTblEntry for the target relation being updated (used for column name resolution)

## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md), lnext
  - [strip_implicit_coercions](../s/strip_implicit_coercions.md)
  - [count_nonjunk_tlist_entries](../c/count_nonjunk_tlist_entries.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [get_attname](get_attname.md)
  - [processIndirection](../p/processIndirection.md)
  - [get_rule_expr](get_rule_expr.md)
- Called from:
  - [get_insert_query_def](get_insert_query_def.md)
  - [get_update_query_def](get_update_query_def.md)
  - [get_merge_query_def](get_merge_query_def.md)

## Notes and Other Information
- This is a static function within ruleutils.c, part of PostgreSQL's rule decompilation system
- The function handles PostgreSQL's advanced multi-assignment syntax for tuple updates
- Column names are resolved from system catalogs to handle RENAME operations correctly
- MULTIEXPR sublinks are identified by their paramkind and processed specially for tuple assignments
- The function processes complex indirection patterns including nested field stores, array subscripts, and domain coercions
- Resjunk entries in the targetList are skipped as they are internal bookkeeping entries
- Part of the broader UPDATE statement deparsing infrastructure used across INSERT, UPDATE, and MERGE operations

## Simplified Source

```c
static void get_update_query_targetlist_def(Query *query, List *targetList,
                                           deparse_context *context, RangeTblEntry *rte) {
    StringInfo buf = context->buf;
    ListCell *l;
    ListCell *next_ma_cell;
    int remaining_ma_columns;
    const char *sep;
    SubLink *cur_ma_sublink;
    List *ma_sublinks;

    // Collect MULTIEXPR sublinks for multi-assignment handling
    ma_sublinks = NIL;
    if (query->hasSubLinks) {
        foreach(l, targetList) {
            TargetEntry *tle = (TargetEntry *) lfirst(l);

            if (tle->resjunk && IsA(tle->expr, SubLink)) {
                SubLink *sl = (SubLink *) tle->expr;

                if (sl->subLinkType == MULTIEXPR_SUBLINK) {
                    ma_sublinks = lappend(ma_sublinks, sl);
                    Assert(sl->subLinkId == list_length(ma_sublinks));
                }
            }
        }
    }
    next_ma_cell = list_head(ma_sublinks);
    cur_ma_sublink = NULL;
    remaining_ma_columns = 0;

    // Generate comma-separated list of 'column = value' assignments
    sep = "";
    foreach(l, targetList) {
        TargetEntry *tle = (TargetEntry *) lfirst(l);
        Node *expr;

        if (tle->resjunk)
            continue;  // Skip junk entries

        appendStringInfoString(buf, sep);
        sep = ", ";

        // Check for start of multi-assignment group
        if (next_ma_cell != NULL && cur_ma_sublink == NULL) {
            // Search for PARAM_MULTIEXPR through indirection layers
            expr = (Node *) tle->expr;
            while (expr) {
                if (IsA(expr, FieldStore)) {
                    FieldStore *fstore = (FieldStore *) expr;
                    expr = (Node *) linitial(fstore->newvals);
                }
                else if (IsA(expr, SubscriptingRef)) {
                    SubscriptingRef *sbsref = (SubscriptingRef *) expr;
                    if (sbsref->refassgnexpr == NULL)
                        break;
                    expr = (Node *) sbsref->refassgnexpr;
                }
                else if (IsA(expr, CoerceToDomain)) {
                    CoerceToDomain *cdomain = (CoerceToDomain *) expr;
                    if (cdomain->coercionformat != COERCE_IMPLICIT_CAST)
                        break;
                    expr = (Node *) cdomain->arg;
                }
                else
                    break;
            }
            expr = strip_implicit_coercions(expr);

            if (expr && IsA(expr, Param) &&
                ((Param *) expr)->paramkind == PARAM_MULTIEXPR) {
                cur_ma_sublink = (SubLink *) lfirst(next_ma_cell);
                next_ma_cell = lnext(ma_sublinks, next_ma_cell);
                remaining_ma_columns = count_nonjunk_tlist_entries(((Query *) cur_ma_sublink->subselect)->targetList);
                Assert(((Param *) expr)->paramid == ((cur_ma_sublink->subLinkId << 16) | 1));
                appendStringInfoChar(buf, '(');
            }
        }

        // Output target column name from system catalog
        appendStringInfoString(buf,
                              quote_identifier(get_attname(rte->relid, tle->resno, false)));

        // Handle field/array indirection
        expr = processIndirection((Node *) tle->expr, context);

        // Handle multi-assignment completion
        if (cur_ma_sublink != NULL) {
            if (--remaining_ma_columns > 0)
                continue;  // Not the last column yet
            appendStringInfoChar(buf, ')');
            expr = (Node *) cur_ma_sublink;
            cur_ma_sublink = NULL;
        }

        appendStringInfoString(buf, " = ");
        get_rule_expr(expr, context, false);
    }
}
```