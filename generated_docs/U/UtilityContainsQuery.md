# UtilityContainsQuery

## Location
[src/backend/tcop/utility.c:2177-2213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L2177-L2213)

## Overview
UtilityContainsQuery extracts and returns the plannable Query contained within utility statements, enabling the system to access and process nested queries for planning and execution.

## Definition

```c
Query *
UtilityContainsQuery(Node *parsetree)
```
## Detailed Description
UtilityContainsQuery is designed to extract plannable Query nodes from utility statements that contain them. Certain utility commands like EXPLAIN, DECLARE CURSOR, and CREATE TABLE AS wrap regular SQL queries, and this function provides a standardized way to access those inner queries.

The function handles several specific utility statement types:
- **DeclareCursorStmt**: Extracts the SELECT query that defines the cursor
- **ExplainStmt**: Extracts the query being explained  
- **CreateTableAsStmt**: Extracts the SELECT query used to populate the new table

The function includes recursive logic to handle nested utility statements. If the contained query is itself a utility command (CMD_UTILITY), it recursively calls itself to drill down until it finds a non-utility Query or returns NULL if none exists.

This capability is particularly important for cases like "EXPLAIN CREATE TABLE AS SELECT" where multiple levels of utility statements are nested.

## Parameters / Member Variables
- : Pointer to a utility statement node that potentially contains a Query

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to identify statement type)
  - castNode (to safely cast nodes to Query type)
  - [UtilityContainsQuery](UtilityContainsQuery.md) (recursive calls for nested utilities)
  - Statement types: DeclareCursorStmt, ExplainStmt, CreateTableAsStmt
  - CMD_UTILITY constant

- Called from:
  - [extract_query_dependencies_walker](../e/extract_query_dependencies_walker.md) (for dependency analysis)
  - [AcquireExecutorLocks](../A/AcquireExecutorLocks.md), AcquirePlannerLocks (for lock management)
  - [UtilityContainsQuery](UtilityContainsQuery.md) (recursive calls)
  - COMMAND_IS_NOT_READ_ONLY macro

## Notes and Other Information
- This function assumes the input parsetree has already been parse-analyzed
- The recursive design handles nested utility statements elegantly
- Returns NULL for utility statements that don't contain queries
- Essential for proper query planning and lock acquisition for nested queries
- Part of the utility command processing infrastructure that bridges utility and regular query processing

## Simplified Source

```c
Query *UtilityContainsQuery(Node *parsetree)
{
    Query      *qry;

    // Check the type of utility statement and extract the contained query
    switch (nodeTag(parsetree))
    {
        case T_DeclareCursorStmt:
            // Extract query from cursor declaration
            qry = castNode(Query, ((DeclareCursorStmt *) parsetree)->query);
            if (qry->commandType == CMD_UTILITY)
                return UtilityContainsQuery(qry->utilityStmt);  // Recursive call
            return qry;

        case T_ExplainStmt:
            // Extract query being explained
            qry = castNode(Query, ((ExplainStmt *) parsetree)->query);
            if (qry->commandType == CMD_UTILITY)
                return UtilityContainsQuery(qry->utilityStmt);  // Recursive call
            return qry;

        case T_CreateTableAsStmt:
            // Extract SELECT query for CREATE TABLE AS
            qry = castNode(Query, ((CreateTableAsStmt *) parsetree)->query);
            if (qry->commandType == CMD_UTILITY)
                return UtilityContainsQuery(qry->utilityStmt);  // Recursive call
            return qry;

        default:
            // Statement doesn't contain a plannable query
            return NULL;
    }
}
```