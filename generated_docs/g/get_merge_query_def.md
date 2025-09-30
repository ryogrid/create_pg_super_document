# get_merge_query_def

## Location
[src/backend/utils/adt/ruleutils.c:7122-7284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L7122-L7284)

## Overview
Generates the text representation of a MERGE SQL statement from a parsed Query structure, reconstructing the complete MERGE command with all its clauses and actions.

## Definition
```c
static void get_merge_query_def(Query *query, deparse_context *context)
```

## Detailed Description
This function is responsible for deparsing (converting back to text) a MERGE query from PostgreSQL's internal Query representation. MERGE is a complex SQL statement that allows conditional INSERT, UPDATE, DELETE, or DO NOTHING operations based on whether rows match between source and target tables.

The function handles the complete MERGE syntax including:
1. **WITH clause**: Common Table Expressions (CTEs)
2. **MERGE INTO target**: The target table specification with aliasing
3. **USING source**: The source table/query specification  
4. **ON condition**: The join condition between source and target
5. **WHEN clauses**: Multiple conditional actions based on match status
6. **Actions**: INSERT, UPDATE, DELETE, or DO NOTHING operations
7. **RETURNING clause**: Optional result set from the operation

Key processing aspects:
- Determines whether to use SQL standard "WHEN NOT MATCHED" vs non-standard "WHEN NOT MATCHED BY TARGET/SOURCE" syntax
- Processes each MergeAction with appropriate match conditions (MATCHED, NOT MATCHED BY SOURCE, NOT MATCHED BY TARGET)
- Handles complex INSERT actions with column lists, VALUES clauses, and OVERRIDING options
- Delegates UPDATE SET clause generation to get_update_query_targetlist_def
- Supports conditional qualifiers (AND conditions) on WHEN clauses

## Parameters / Member Variables
- `query`: The Query structure containing the parsed MERGE statement to be deparsed
- `context`: The deparse_context containing formatting preferences, indentation level, and the output StringInfo buffer

## Dependencies
- Functions called/Symbols referenced:
  - [get_with_clause](get_with_clause.md)
  - rt_fetch
  - only_marker
  - [generate_relation_name](generate_relation_name.md)
  - [get_rte_alias](get_rte_alias.md)
  - [get_from_clause](get_from_clause.md)
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - [get_rule_expr](get_rule_expr.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [get_attname](get_attname.md)
  - [processIndirection](../p/processIndirection.md)
  - [get_rule_list_toplevel](get_rule_list_toplevel.md)
  - [get_update_query_targetlist_def](get_update_query_targetlist_def.md)
  - [get_target_list](get_target_list.md)
- Called from:
  - [get_query_def](get_query_def.md)

## Notes and Other Information
- This is a static function within ruleutils.c, part of PostgreSQL's rule decompilation system
- The function handles both SQL standard and PostgreSQL-specific MERGE syntax variations
- When NOT MATCHED BY SOURCE actions are present, all NOT MATCHED actions are explicitly qualified for clarity
- INSERT actions within MERGE support the full range of INSERT features including column specification, VALUES vs DEFAULT VALUES, and OVERRIDING clauses
- UPDATE actions reuse the existing targetlist deparsing infrastructure through get_update_query_targetlist_def
- The function assumes the query is a valid MERGE query with proper MergeAction list structure
- Part of the broader query deparsing infrastructure used for rule definitions, view definitions, and query display
- MERGE is a relatively recent addition to PostgreSQL (version 15+) and represents one of the most complex SQL statement types

## Simplified Source
```c
static void get_merge_query_def(Query *query, deparse_context *context) {
    StringInfo buf = context->buf;
    RangeTblEntry *rte;
    ListCell *lc;
    bool haveNotMatchedBySource = false;

    // Add WITH clause if present
    get_with_clause(query, context);

    // Start with MERGE INTO target_table
    rte = rt_fetch(query->resultRelation, query->rtable);

    if (PRETTY_INDENT(context)) {
        appendStringInfoChar(buf, ' ');
        context->indentLevel += PRETTYINDENT_STD;
    }

    appendStringInfo(buf, "MERGE INTO %s%s",
                     only_marker(rte),
                     generate_relation_name(rte->relid, NIL));

    // Add relation alias if needed
    get_rte_alias(rte, query->resultRelation, false, context);

    // Add USING clause and ON condition
    get_from_clause(query, " USING ", context);
    appendContextKeyword(context, " ON ",
                       -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);
    get_rule_expr(query->mergeJoinCondition, context, false);

    // Check if we have NOT MATCHED BY SOURCE actions (affects syntax)
    foreach(lc, query->mergeActionList) {
        MergeAction *action = lfirst_node(MergeAction, lc);
        if (action->matchKind == MERGE_WHEN_NOT_MATCHED_BY_SOURCE) {
            haveNotMatchedBySource = true;
            break;
        }
    }

    // Process each WHEN clause
    foreach(lc, query->mergeActionList) {
        MergeAction *action = lfirst_node(MergeAction, lc);

        appendContextKeyword(context, " WHEN ",
                           -PRETTYINDENT_STD, PRETTYINDENT_STD, 2);

        // Add match condition type
        switch (action->matchKind) {
            case MERGE_WHEN_MATCHED:
                appendStringInfoString(buf, "MATCHED");
                break;
            case MERGE_WHEN_NOT_MATCHED_BY_SOURCE:
                appendStringInfoString(buf, "NOT MATCHED BY SOURCE");
                break;
            case MERGE_WHEN_NOT_MATCHED_BY_TARGET:
                if (haveNotMatchedBySource)
                    appendStringInfoString(buf, "NOT MATCHED BY TARGET");
                else
                    appendStringInfoString(buf, "NOT MATCHED");
                break;
        }

        // Add optional AND condition
        if (action->qual) {
            appendContextKeyword(context, " AND ",
                               -PRETTYINDENT_STD, PRETTYINDENT_STD, 3);
            get_rule_expr(action->qual, context, false);
        }

        appendContextKeyword(context, " THEN ",
                           -PRETTYINDENT_STD, PRETTYINDENT_STD, 3);

        // Add the action
        if (action->commandType == CMD_INSERT) {
            List *strippedexprs = NIL;
            const char *sep = "";

            appendStringInfoString(buf, "INSERT");

            // Add column list
            if (action->targetList)
                appendStringInfoString(buf, " (");

            ListCell *lc2;
            foreach(lc2, action->targetList) {
                TargetEntry *tle = (TargetEntry *) lfirst(lc2);

                appendStringInfoString(buf, sep);
                sep = ", ";

                appendStringInfoString(buf,
                    quote_identifier(get_attname(rte->relid, tle->resno, false)));
                strippedexprs = lappend(strippedexprs,
                                      processIndirection((Node *) tle->expr, context));
            }

            if (action->targetList)
                appendStringInfoChar(buf, ')');

            // Add OVERRIDING clause
            if (action->override) {
                if (action->override == OVERRIDING_SYSTEM_VALUE)
                    appendStringInfoString(buf, " OVERRIDING SYSTEM VALUE");
                else if (action->override == OVERRIDING_USER_VALUE)
                    appendStringInfoString(buf, " OVERRIDING USER VALUE");
            }

            // Add VALUES or DEFAULT VALUES
            if (strippedexprs) {
                appendContextKeyword(context, " VALUES (",
                                   -PRETTYINDENT_STD, PRETTYINDENT_STD, 4);
                get_rule_list_toplevel(strippedexprs, context, false);
                appendStringInfoChar(buf, ')');
            }
            else {
                appendStringInfoString(buf, " DEFAULT VALUES");
            }
        }
        else if (action->commandType == CMD_UPDATE) {
            appendStringInfoString(buf, "UPDATE SET ");
            get_update_query_targetlist_def(query, action->targetList, context, rte);
        }
        else if (action->commandType == CMD_DELETE) {
            appendStringInfoString(buf, "DELETE");
        }
        else if (action->commandType == CMD_NOTHING) {
            appendStringInfoString(buf, "DO NOTHING");
        }
    }

    // Add RETURNING clause if present
    if (query->returningList) {
        appendContextKeyword(context, " RETURNING",
                           -PRETTYINDENT_STD, PRETTYINDENT_STD, 1);
        get_target_list(query->returningList, context);
    }
}
```