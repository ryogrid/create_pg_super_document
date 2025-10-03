# PrepareQuery

## Location
[src/backend/commands/prepare.c:56-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L56-L146)

## Overview
Implements the 'PREPARE' utility statement, creating a named prepared statement by parsing, analyzing, and storing a SQL statement with optional parameter types for later execution.

## Definition

```c
void
PrepareQuery(ParseState *pstate, PrepareStmt *stmt,
			 int stmt_location, int stmt_len)
```
## Detailed Description
PrepareQuery processes a PREPARE SQL statement by creating a cached plan source that can be executed multiple times with different parameter values. The function validates the statement name, wraps the query in a RawStmt node, performs parse analysis and query rewriting, and stores the resulting prepared statement for future use. It handles parameter type resolution either from explicitly specified types or by inferring them from the query context.

## Parameters / Member Variables
- `*pstate`: Parse state containing parsing context and source text information
- `*stmt`: PrepareStmt node containing the statement name, query, and optional parameter types
- `stmt_location`: Starting location of the statement in the source text
- `stmt_len`: Length of the statement in the source text
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates RawStmt)
  - [CreateCachedPlan](../C/CreateCachedPlan.md) (creates cached plan source)
  - [CreateCommandTag](../C/CreateCommandTag.md) (generates command tag)
  - [typenameTypeId](../t/typenameTypeId.md) (resolves type names to OIDs)
  - [pg_analyze_and_rewrite_varparams](../p/pg_analyze_and_rewrite_varparams.md) (performs analysis and rewriting)
  - [CompleteCachedPlan](../C/CompleteCachedPlan.md) (finalizes cached plan)
  - [StorePreparedStatement](../S/StorePreparedStatement.md) (stores the prepared statement)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processing)

## Notes and Other Information
- Disallows empty statement names to avoid conflicts with protocol-level unnamed statements
- Supports parameter type inference when explicit types are not provided
- Creates reusable cached plans that can improve performance for repeated executions
- Allows parallel execution mode for compatible queries
- Part of PostgreSQL's prepared statement infrastructure for optimizing repeated query execution

## Simplified Source

```c
void PrepareQuery(ParseState *pstate, PrepareStmt *stmt,
                 int stmt_location, int stmt_len) {
    RawStmt *rawstmt;
    CachedPlanSource *plansource;
    Oid *argtypes = NULL;
    int nargs;
    List *query_list;

    // Validate statement name - must not be empty
    if (!stmt->name || stmt->name[0] == '\0') {
        ereport(ERROR, "invalid statement name: must not be empty");
    }

    // Wrap the statement in a RawStmt for parse analysis
    rawstmt = makeNode(RawStmt);
    rawstmt->stmt = stmt->query;
    rawstmt->stmt_location = stmt_location;
    rawstmt->stmt_len = stmt_len;

    // Create cached plan source before parse analysis
    plansource = CreateCachedPlan(rawstmt, pstate->p_sourcetext,
                                 CreateCommandTag(stmt->query));

    // Convert parameter type names to OIDs
    nargs = list_length(stmt->argtypes);
    if (nargs) {
        int i = 0;
        ListCell *l;

        argtypes = palloc_array(Oid, nargs);
        foreach(l, stmt->argtypes) {
            TypeName *tn = lfirst(l);
            Oid toid = typenameTypeId(pstate, tn);
            argtypes[i++] = toid;
        }
    }

    // Analyze and rewrite the query with parameter types
    query_list = pg_analyze_and_rewrite_varparams(rawstmt, pstate->p_sourcetext,
                                                 &argtypes, &nargs, NULL);

    // Complete the cached plan with query list and parameters
    CompleteCachedPlan(plansource, query_list, NULL, argtypes, nargs,
                      NULL, NULL, CURSOR_OPT_PARALLEL_OK, true);

    // Store the prepared statement for later execution
    StorePreparedStatement(stmt->name, plansource, true);
}
```