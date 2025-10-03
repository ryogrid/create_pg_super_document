# parse_analyze_withcb

## Location
[src/backend/parser/analyze.c:185-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L185-L220)

## Overview
Analyzes a raw parse tree using a caller-supplied parser setup callback, allowing for custom parameter resolution and other specialized parsing configurations.

## Definition

```c
Query *
parse_analyze_withcb(RawStmt *parseTree, const char *sourceText,
					 ParserSetupHook parserSetup,
					 void *parserSetupArg,
					 QueryEnvironment *queryEnv)
```
## Detailed Description
This function provides the most flexible variant of the parse analysis functions by allowing callers to supply their own parser setup callback. The callback mechanism enables custom parameter resolution strategies and other specialized parsing configurations that may be needed for specific use cases.

The function follows a streamlined workflow:
1. Creates a parse state structure
2. Sets up the query environment
3. Invokes the caller-provided setup callback with the parse state
4. Performs statement transformation
5. Generates query ID and processes hooks as standard

This approach is particularly useful for extensions, procedural languages, or other components that need specialized parameter handling or parsing behavior that differs from the standard PostgreSQL approaches.

## Parameters / Member Variables
- `*parseTree`: The raw parse tree structure produced by the SQL parser
- `*sourceText`: The original SQL source text (required as of PostgreSQL 8.4)
- `parserSetup`: Callback function pointer for custom parser setup
- `*parserSetupArg`: Argument to pass to the parser setup callback
- `*queryEnv`: Query environment containing additional context like WITH clause data
## Dependencies
- Functions called/Symbols referenced:
  - [make_parsestate](../m/make_parsestate.md): Creates parse state structure
  - [transformTopLevelStmt](../t/transformTopLevelStmt.md): Performs the main statement transformation
  - [IsQueryIdEnabled](../I/IsQueryIdEnabled.md): Checks if query ID generation is enabled
  - [JumbleQuery](../J/JumbleQuery.md): Generates query ID for statistics
  - [free_parsestate](../f/free_parsestate.md): Cleanup parse state structure
  - [pgstat_report_query_id](pgstat_report_query_id.md): Reports query ID for statistics collection
  - ParserSetupHook: Type for the parser setup callback function

- Called from (representative examples):
  - [pg_analyze_and_rewrite_withcb](pg_analyze_and_rewrite_withcb.md): Main analysis entry point with callback

## Notes and Other Information
- This is the most flexible of the three parse_analyze variants
- The parser setup callback allows complete customization of parameter handling
- Commonly used by extensions and procedural languages (PL/pgSQL, etc.)
- The callback receives the ParseState and setup argument for configuration
- Does not include built-in parameter validation like the other variants
- Maintains compatibility with standard query ID generation and post-parse hooks
- The setup callback is responsible for all parameter-related configuration

## Simplified Source

```c
Query *parse_analyze_withcb(RawStmt *parseTree, const char *sourceText,
                           ParserSetupHook parserSetup,
                           void *parserSetupArg,
                           QueryEnvironment *queryEnv)
{
    ParseState *pstate = make_parsestate(NULL);
    Query      *query;
    JumbleState *jstate = NULL;

    // Source text is required (as of PostgreSQL 8.4)
    Assert(sourceText != NULL);

    // Set up the parse state with source text and query environment
    pstate->p_sourcetext = sourceText;
    pstate->p_queryEnv = queryEnv;

    // Call the custom parser setup callback
    (*parserSetup) (pstate, parserSetupArg);

    // Transform the raw statement into a Query
    query = transformTopLevelStmt(pstate, parseTree);

    // Generate query ID if enabled
    if (IsQueryIdEnabled())
        jstate = JumbleQuery(query);

    // Execute post-parse analysis hook if registered
    if (post_parse_analyze_hook)
        (*post_parse_analyze_hook) (pstate, query, jstate);

    // Clean up parse state
    free_parsestate(pstate);

    // Report query ID for statistics
    pgstat_report_query_id(query->queryId, false);

    return query;
}
```