# ExplainOneUtility

## Location
[src/backend/commands/explain.c:527-616](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L527-L616)

## Overview
ExplainOneUtility prints out execution plans for utility statements that have special handling in the EXPLAIN system, such as CREATE TABLE AS, DECLARE CURSOR, EXECUTE, and NOTIFY statements.

## Definition

```c
structure\n");
```
## Detailed Description
ExplainOneUtility handles the explanation of utility statements that require special processing in PostgreSQL's EXPLAIN system. While most utility statements don't have execution plans, certain statements like CREATE TABLE AS and DECLARE CURSOR contain embedded SELECT queries that do have plans and need to be explained.

The function identifies the type of utility statement and processes each accordingly:
- For CREATE TABLE AS and CREATE MATERIALIZED VIEW: Rewrites the contained SELECT query and recursively calls ExplainOneQuery
- For DECLARE CURSOR: Similarly extracts and explains the embedded query
- For EXECUTE: Delegates to ExplainExecuteQuery for prepared statement explanation
- For NOTIFY: Outputs a simple notification message
- For other utilities: Indicates that no plan structure exists

The function is exported because it's called from prepare.c in EXPLAIN EXECUTE scenarios, where statements are retrieved from the plan cache and must not be modified.

## Parameters / Member Variables
- : The utility statement node to be explained
- : IntoClause for CREATE TABLE AS statements, NULL otherwise
- : ExplainState containing output formatting and options
- : Original query string for context
- : Parameter list for parameterized queries
- : Query environment for additional context

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTableAsRelExists](../C/CreateTableAsRelExists.md)
  - [ExplainDummyGroup](ExplainDummyGroup.md)
  - [QueryRewrite](../Q/QueryRewrite.md)
  - copyObject
  - [ExplainOneQuery](ExplainOneQuery.md)
  - [ExplainExecuteQuery](ExplainExecuteQuery.md)
  - linitial_node
  - [appendStringInfoString](../a/appendStringInfoString.md)
- Called from (representative examples):
  - [ExplainOneQuery](ExplainOneQuery.md)
  - [ExplainExecuteQuery](ExplainExecuteQuery.md)

## Notes and Other Information
- The function performs existence checks for CREATE TABLE AS to avoid unnecessary planning when the target relation already exists
- For DECLARE CURSOR with EXPLAIN ANALYZE, the query actually executes but no cursor is created
- The function carefully handles plan cache scenarios by copying statements to avoid modification
- Different output formats (text vs structured) are handled appropriately for each statement type

## Simplified Source

```c
void ExplainOneUtility(Node *utilityStmt, IntoClause *into, ExplainState *es,
                       const char *queryString, ParamListInfo params,
                       QueryEnvironment *queryEnv)
{
    if (utilityStmt == NULL)
        return;

    if (IsA(utilityStmt, CreateTableAsStmt))
    {
        CreateTableAsStmt *ctas = (CreateTableAsStmt *) utilityStmt;

        // Check if target relation already exists
        if (CreateTableAsRelExists(ctas))
        {
            if (ctas->objtype == OBJECT_TABLE)
                ExplainDummyGroup("CREATE TABLE AS", NULL, es);
            else if (ctas->objtype == OBJECT_MATVIEW)
                ExplainDummyGroup("CREATE MATERIALIZED VIEW", NULL, es);
            else
                elog(ERROR, "unexpected object type: %d", (int) ctas->objtype);
            return;
        }

        // Rewrite and explain the contained SELECT query
        List *rewritten = QueryRewrite(castNode(Query, copyObject(ctas->query)));
        Assert(list_length(rewritten) == 1);
        ExplainOneQuery(linitial_node(Query, rewritten),
                        CURSOR_OPT_PARALLEL_OK, ctas->into, es,
                        queryString, params, queryEnv);
    }
    else if (IsA(utilityStmt, DeclareCursorStmt))
    {
        // Extract and explain the cursor's query
        DeclareCursorStmt *dcs = (DeclareCursorStmt *) utilityStmt;
        List *rewritten = QueryRewrite(castNode(Query, copyObject(dcs->query)));
        Assert(list_length(rewritten) == 1);
        ExplainOneQuery(linitial_node(Query, rewritten),
                        dcs->options, NULL, es,
                        queryString, params, queryEnv);
    }
    else if (IsA(utilityStmt, ExecuteStmt))
    {
        ExplainExecuteQuery((ExecuteStmt *) utilityStmt, into, es,
                            queryString, params, queryEnv);
    }
    else if (IsA(utilityStmt, NotifyStmt))
    {
        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfoString(es->str, "NOTIFY\n");
        else
            ExplainDummyGroup("Notify", NULL, es);
    }
    else
    {
        // Generic utility statement with no plan structure
        if (es->format == EXPLAIN_FORMAT_TEXT)
            appendStringInfoString(es->str,
                                   "Utility statements have no plan structure\n");
        else
            ExplainDummyGroup("Utility Statement", NULL, es);
    }
}
```