# get_target_list

## Location
[src/backend/utils/adt/ruleutils.c:6035-6170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L6035-L6170)

## Overview
Parses back a SELECT target list into SQL text format, also used for RETURNING lists in INSERT/UPDATE/DELETE/MERGE statements.

## Definition
```c
static void get_target_list(List *targetList, deparse_context *context)
```

## Detailed Description
This function converts a list of TargetEntry nodes back into SQL text representation. It handles each target list entry by determining the appropriate column expression and alias. The function has special handling for Var nodes to avoid expanding whole-row variables into multiple columns at the top level of a SELECT list.

Key features include:
- Skips junk entries (internal entries not visible in SQL output)
- Special-cases Var nodes to prevent inappropriate expansion of whole-row variables
- Determines appropriate column names from view descriptors or TargetEntry names
- Adds AS clauses when necessary to preserve column naming
- Handles line wrapping and formatting based on context settings
- Manages comma separation between target list items

The function uses a temporary buffer to format each target entry before deciding on line wrapping, ensuring proper SQL formatting.

## Parameters / Member Variables
- `targetList`: List of TargetEntry nodes representing the SELECT target list or RETURNING clause
- `context`: deparse_context containing formatting options, output buffer, and view information

## Dependencies
- Functions called/Symbols referenced:
  - [get_variable](get_variable.md) (get text for Var nodes with proper whole-row handling)
  - [get_rule_expr](get_rule_expr.md) (get text for general expression nodes)
  - [quote_identifier](../q/quote_identifier.md) (properly quote SQL identifiers)
  - [resetStringInfo](../r/resetStringInfo.md) (clear temporary string buffer)
  - [removeStringInfoSpaces](../r/removeStringInfoSpaces.md) (formatting utility)
  - [appendContextKeyword](../a/appendContextKeyword.md) (add keywords with proper indentation)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (append formatted text to output buffer)
- Called from (representative examples):
  - [get_basic_select_query](get_basic_select_query.md) (src/backend/utils/adt/ruleutils.c:5960)
  - [get_insert_query_def](get_insert_query_def.md) (src/backend/utils/adt/ruleutils.c:6853)
  - [get_update_query_def](get_update_query_def.md) (src/backend/utils/adt/ruleutils.c:6909)
  - [get_delete_query_def](get_delete_query_def.md) (src/backend/utils/adt/ruleutils.c:7112)
  - [get_merge_query_def](get_merge_query_def.md) (src/backend/utils/adt/ruleutils.c:7275)

## Notes and Other Information
- Critical component of PostgreSQL's rule decompilation system
- Handles view column renaming by using resultDesc when available
- Prevents whole-row Var expansion that would change query semantics at SELECT list level
- Supports intelligent line wrapping for better SQL readability
- Used across multiple statement types (SELECT, INSERT, UPDATE, DELETE, MERGE) for consistent target list formatting
- Manages AS clause generation to preserve original column naming intent

## Simplified Source

```c
static void get_target_list(List *targetList, deparse_context *context) {
    StringInfo buf = context->buf;
    StringInfoData targetbuf;
    bool last_was_multiline = false;
    char *sep;
    int colno;
    ListCell *l;

    // Initialize temporary buffer for target entries
    initStringInfo(&targetbuf);

    sep = " ";
    colno = 0;
    foreach(l, targetList) {
        TargetEntry *tle = (TargetEntry *) lfirst(l);
        char *colname;
        char *attname;

        if (tle->resjunk)
            continue;  // Skip junk entries

        appendStringInfoString(buf, sep);
        sep = ", ";
        colno++;

        // Use temporary buffer to format this target entry
        resetStringInfo(&targetbuf);
        context->buf = &targetbuf;

        // Special handling for Var nodes to avoid whole-row expansion
        if (tle->expr && (IsA(tle->expr, Var))) {
            attname = get_variable((Var *) tle->expr, 0, true, context);
        }
        else {
            get_rule_expr((Node *) tle->expr, context, true);
            attname = context->colNamesVisible ? NULL : "?column?";
        }

        // Determine the result column name
        if (context->resultDesc && colno <= context->resultDesc->natts)
            colname = NameStr(TupleDescAttr(context->resultDesc, colno - 1)->attname);
        else
            colname = tle->resname;

        // Add AS clause if column name differs
        if (colname) {
            if (attname == NULL || strcmp(attname, colname) != 0)
                appendStringInfo(&targetbuf, " AS %s", quote_identifier(colname));
        }

        // Restore output buffer
        context->buf = buf;

        // Handle line wrapping if enabled
        if (PRETTY_INDENT(context) && context->wrapColumn >= 0) {
            int leading_nl_pos = (targetbuf.len > 0 && targetbuf.data[0] == '\n') ? 0 : -1;

            if (leading_nl_pos >= 0) {
                removeStringInfoSpaces(buf);
            } else {
                char *trailing_nl = strrchr(buf->data, '\n');
                if (trailing_nl == NULL)
                    trailing_nl = buf->data;
                else
                    trailing_nl++;

                // Add newline if needed for overflow or multiline fields
                if (colno > 1 &&
                    ((strlen(trailing_nl) + targetbuf.len > context->wrapColumn) ||
                     last_was_multiline))
                    appendContextKeyword(context, "", -PRETTYINDENT_STD,
                                       PRETTYINDENT_STD, PRETTYINDENT_VAR);
            }

            // Track multiline status for next iteration
            last_was_multiline = (strchr(targetbuf.data + leading_nl_pos + 1, '\n') != NULL);
        }

        // Add the formatted target entry
        appendBinaryStringInfo(buf, targetbuf.data, targetbuf.len);
    }

    pfree(targetbuf.data);
}
```