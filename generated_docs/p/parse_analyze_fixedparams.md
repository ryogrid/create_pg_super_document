# parse_analyze_fixedparams

## Location
[src/backend/parser/analyze.c:104-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L104-L143)

## Overview
Analyzes a raw parse tree and transforms it into a Query node, with support for pre-defined parameter types where references to undefined parameter indexes are disallowed.

## Definition

```c
Query *
parse_analyze_fixedparams(RawStmt *parseTree, const char *sourceText,
						  const Oid *paramTypes, int numParams,
						  QueryEnvironment *queryEnv)
```
## Detailed Description
This function serves as one of the main entry points for SQL parsing and analysis in PostgreSQL. It takes a raw parse tree (produced by the parser) and transforms it into a fully analyzed Query node that can be used by the planner and executor. The function specifically handles cases where parameter types are known in advance, making it suitable for prepared statements and similar scenarios where parameter information is available upfront.

The function performs several key operations:
1. Creates a parse state structure to track parsing context
2. Sets up parameter type information if provided
3. Transforms the raw statement using the top-level transformation logic
4. Generates a query ID for statistics if enabled
5. Invokes post-parse analysis hooks if configured
6. Reports query statistics

## Parameters / Member Variables
- `*parseTree`: The raw parse tree structure produced by the SQL parser
- `*sourceText`: The original SQL source text (required as of PostgreSQL 8.4)
- `*paramTypes`: Array of parameter type OIDs for  parameter references
- `numParams`: Number of parameters in the paramTypes array
- `*queryEnv`: Query environment containing additional context like WITH clause data
## Dependencies
- Functions called/Symbols referenced:
  - [make_parsestate](../m/make_parsestate.md): Creates parse state structure
  - [setup_parse_fixed_parameters](../s/setup_parse_fixed_parameters.md): Sets up parameter type information
  - [transformTopLevelStmt](../t/transformTopLevelStmt.md): Performs the main statement transformation
  - [IsQueryIdEnabled](../I/IsQueryIdEnabled.md): Checks if query ID generation is enabled
  - [JumbleQuery](../J/JumbleQuery.md): Generates query ID for statistics
  - [free_parsestate](../f/free_parsestate.md): Cleanup parse state structure
  - [pgstat_report_query_id](pgstat_report_query_id.md): Reports query ID for statistics collection

- Called from (representative examples):
  - [DefineView](../D/DefineView.md): Used when creating views
  - [pg_analyze_and_rewrite_fixedparams](pg_analyze_and_rewrite_fixedparams.md): Main analysis entry point in postgres.c

## Notes and Other Information
- This function is part of the parse analysis phase, distinct from the raw parsing phase
- The function ensures that only pre-defined parameters (via paramTypes) can be referenced
- Post-parse analysis hooks allow extensions to modify or inspect the analyzed query
- [Query](../Q/Query.md) ID generation supports query statistics and monitoring features
- The function handles both optimizable SQL statements and utility statements differently

## Simplified Source

```c
// Simplified version of parse_analyze_fixedparams
Query *
parse_analyze_fixedparams(RawStmt *parseTree, const char *sourceText,
                          const Oid *paramTypes, int numParams,
                          QueryEnvironment *queryEnv)
{
    ParseState *pstate = make_parsestate(NULL);
    Query *query;
    JumbleState *jstate = NULL;

    Assert(sourceText != NULL);

    // Set up parse state with source text
    pstate->p_sourcetext = sourceText;

    // Set up parameter types if provided
    if (numParams > 0)
        setup_parse_fixed_parameters(pstate, paramTypes, numParams);

    // Set query environment
    pstate->p_queryEnv = queryEnv;

    // Transform the raw statement into a Query node
    query = transformTopLevelStmt(pstate, parseTree);

    // Generate query ID for statistics if enabled
    if (IsQueryIdEnabled())
        jstate = JumbleQuery(query);

    // Call post-parse analysis hook if configured
    if (post_parse_analyze_hook)
        (*post_parse_analyze_hook)(pstate, query, jstate);

    // Clean up parse state
    free_parsestate(pstate);

    // Report query ID for statistics
    pgstat_report_query_id(query->queryId, false);

    return query;
}
```

Key simplifications made:
- Added comments explaining each major phase of analysis
- Clarified the purpose of parameter setup and query environment
- Preserved all essential functionality including hooks and statistics
- Maintained exact control flow and error handling