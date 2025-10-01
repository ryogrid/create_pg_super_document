# get_from_clause

## Location
[src/backend/utils/adt/ruleutils.c:11940-12033](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11940-L12033)

## Overview
Reconstructs FROM clauses (or USING clauses for DELETE) from the query's join tree during SQL statement deparsing, handling formatting, line wrapping, and filtering of auto-added range table entries.

## Definition
```c
static void get_from_clause(Query *query, const char *prefix, deparse_context *context)
```

## Detailed Description
This function is responsible for regenerating FROM clauses in SELECT and UPDATE statements, as well as USING clauses in DELETE statements. It processes the query's joint tree structure and formats the output with proper SQL syntax. Key functionality includes:

1. **Join tree traversal**: Iterates through the query's jointree fromlist to determine what to include
2. **RTE filtering**: Excludes auto-added range table entries marked as not inFromCl, including rule pseudo-RTEs for NEW and OLD
3. **Formatting and indentation**: Handles proper keyword placement, indentation, and comma separation
4. **Line wrapping**: Implements intelligent line wrapping when enabled, considering wrap column limits
5. **Buffer management**: Uses temporary buffers to assess formatting needs before committing output
6. **Flexible prefix**: Supports different keywords (FROM for SELECT/UPDATE, USING for DELETE)

The function maintains proper SQL syntax while providing readable formatting through PostgreSQL's pretty-printing system.

## Parameters / Member Variables
- `query`: Query structure containing the jointree and range table information to be processed
- `prefix`: String keyword that starts the clause ("FROM" for SELECT/UPDATE, "USING" for DELETE statements)  
- `context`: deparse_context containing output buffer, formatting preferences, indentation levels, and wrap column settings

## Dependencies
- Functions called/Symbols referenced:
  - foreach (macro for list iteration)
  - lfirst (list access macro)
  - IsA (type checking macro)
  - rt_fetch (range table access function)
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - [appendStringInfoString](../a/appendStringInfoString.md), appendBinaryStringInfo
  - [get_from_clause_item](get_from_clause_item.md) (processes individual FROM items)
  - PRETTY_INDENT (formatting macro)
  - PRETTYINDENT_STD, PRETTYINDENT_VAR (indentation constants)
  - [initStringInfo](../i/initStringInfo.md), pfree (string buffer management)
  - [removeStringInfoSpaces](../r/removeStringInfoSpaces.md)
  - strrchr, strlen (standard C string functions)
- Called from (representative examples):
  - [get_basic_select_query](get_basic_select_query.md)
  - [get_update_query_def](get_update_query_def.md)  
  - [get_delete_query_def](get_delete_query_def.md)
  - [get_merge_query_def](get_merge_query_def.md)

## Notes and Other Information
- This is a static function used internally by the rule deparsing system
- The function handles complex formatting logic including intelligent line wrapping based on content length
- Auto-added RTEs are filtered out to avoid including internal PostgreSQL constructs in the output
- The temporary buffer technique allows the function to make formatting decisions based on content size
- Supports both pretty-printed and compact output formats
- Part of PostgreSQL's comprehensive query deparsing system used for view definitions, rule actions, and debugging
- The function preserves semantic correctness while optimizing readability

## Simplified Source

```c
static void get_from_clause(Query *query, const char *prefix, deparse_context *context) {
    StringInfo buf = context->buf;
    bool first = true;
    ListCell *l;

    // Iterate through query's joint tree fromlist
    foreach(l, query->jointree->fromlist) {
        Node *jtnode = (Node *) lfirst(l);

        // Skip auto-added RTEs not marked for inclusion in FROM clause
        if (IsA(jtnode, RangeTblRef)) {
            int varno = ((RangeTblRef *) jtnode)->rtindex;
            RangeTblEntry *rte = rt_fetch(varno, query->rtable);

            if (!rte->inFromCl)
                continue;
        }

        if (first) {
            // Output prefix keyword (FROM/USING) and first item
            appendContextKeyword(context, prefix,
                                -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
            first = false;
            get_from_clause_item(jtnode, query, context);
        }
        else {
            // Handle subsequent items with comma separation and line wrapping
            StringInfoData itembuf;

            appendStringInfoString(buf, ", ");

            // Use temporary buffer to evaluate formatting needs
            initStringInfo(&itembuf);
            context->buf = &itembuf;
            get_from_clause_item(jtnode, query, context);
            context->buf = buf;

            // Apply line wrapping logic if enabled
            if (PRETTY_INDENT(context) && context->wrapColumn >= 0) {
                if (itembuf.len > 0 && itembuf.data[0] == '\n') {
                    removeStringInfoSpaces(buf);
                } else {
                    char *trailing_nl = strrchr(buf->data, '\n');
                    if (trailing_nl == NULL)
                        trailing_nl = buf->data;
                    else
                        trailing_nl++;

                    // Add newline if content would exceed wrap column
                    if (strlen(trailing_nl) + itembuf.len > context->wrapColumn)
                        appendContextKeyword(context, "", -PRETTYINDENT_STD,
                                           PRETTYINDENT_STD, PRETTYINDENT_VAR);
                }
            }

            // Append the formatted item and cleanup
            appendBinaryStringInfo(buf, itembuf.data, itembuf.len);
            pfree(itembuf.data);
        }
    }
}
```