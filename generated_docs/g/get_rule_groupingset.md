# get_rule_groupingset

## Location
[src/backend/utils/adt/ruleutils.c:6388-6447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L6388-L6447)

## Overview
Formats and outputs a GroupingSet clause as a string representation for SQL rule deparsing, handling different types of grouping operations including simple grouping, ROLLUP, CUBE, and GROUPING SETS.

## Definition

```c
static void
get_rule_groupingset(GroupingSet *gset, List *targetlist,
					 bool omit_parens, deparse_context *context)
```
## Detailed Description
This function is a recursive SQL deparsing utility that converts internal GroupingSet structures into their textual SQL representation. It handles various SQL grouping constructs:

- **GROUPING_SET_EMPTY**: Outputs empty parentheses "()"
- **GROUPING_SET_SIMPLE**: Standard grouping expressions with optional parentheses
- **GROUPING_SET_ROLLUP**: ROLLUP() grouping operation
- **GROUPING_SET_CUBE**: CUBE() grouping operation  
- **GROUPING_SET_SETS**: GROUPING SETS() construct containing nested grouping sets

The function recursively processes nested grouping sets and formats column references through the target list, building the output string in the provided context buffer.

## Parameters / Member Variables
- `*gset`: Pointer to the GroupingSet structure containing the grouping specification
- `*targetlist`: List of target expressions used to resolve column references
- `omit_parens`: Boolean flag indicating whether to omit outer parentheses for simple single-element groups
- `*context`: Deparse context containing the output buffer and formatting state
## Dependencies
- Functions called/Symbols referenced:
  - [get_rule_sortgroupclause](get_rule_sortgroupclause.md) (for resolving individual grouping columns)
  - lfirst_int (for extracting integer values from list cells)
  - [appendStringInfoString](../a/appendStringInfoString.md)/appendStringInfoChar (for string buffer operations)
- Called from (representative examples):
  - [get_basic_select_query](get_basic_select_query.md) (for GROUP BY clause generation)
  - [get_rule_groupingset](get_rule_groupingset.md) (recursive calls for nested grouping sets)

## Notes and Other Information
- The function is static and only accessible within ruleutils.c
- Uses recursive descent to handle nested GROUPING SETS structures
- Located at src/backend/utils/adt/ruleutils.c:6388-6447
- Part of PostgreSQL's query deparsing infrastructure for rule and view definitions

## Simplified Source

```c
static void
get_rule_groupingset(GroupingSet *gset, List *targetlist,
                     bool omit_parens, deparse_context *context)
{
    ListCell *l;
    StringInfo buf = context->buf;
    bool omit_child_parens = true;
    char *sep = "";

    switch (gset->kind)
    {
        case GROUPING_SET_EMPTY:
            // Empty grouping set: ()
            appendStringInfoString(buf, "()");
            return;

        case GROUPING_SET_SIMPLE:
            // Simple grouping: (col1, col2, ...)
            if (!omit_parens || list_length(gset->content) != 1)
                appendStringInfoChar(buf, '(');

            foreach(l, gset->content)
            {
                Index ref = lfirst_int(l);
                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause(ref, targetlist, false, context);
                sep = ", ";
            }

            if (!omit_parens || list_length(gset->content) != 1)
                appendStringInfoChar(buf, ')');
            return;

        case GROUPING_SET_ROLLUP:
            appendStringInfoString(buf, "ROLLUP(");
            break;

        case GROUPING_SET_CUBE:
            appendStringInfoString(buf, "CUBE(");
            break;

        case GROUPING_SET_SETS:
            appendStringInfoString(buf, "GROUPING SETS (");
            omit_child_parens = false;
            break;
    }

    // Process nested grouping sets recursively
    foreach(l, gset->content)
    {
        appendStringInfoString(buf, sep);
        get_rule_groupingset(lfirst(l), targetlist, omit_child_parens, context);
        sep = ", ";
    }

    appendStringInfoChar(buf, ')');
}
```