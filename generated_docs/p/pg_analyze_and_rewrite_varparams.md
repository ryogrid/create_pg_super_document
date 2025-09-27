# pg_analyze_and_rewrite_varparams

## Location
[src/backend/tcop/postgres.c:714-767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L714-L767)

## Overview
Performs parse analysis and rule rewriting on a raw parse tree with variable parameter types, allowing parameter types to be deduced from context.

## Definition
```c
List *pg_analyze_and_rewrite_varparams(RawStmt *parsetree,
                                      const char *query_string,
                                      Oid **paramTypes,
                                      int *numParams,
                                      QueryEnvironment *queryEnv)
```

## Detailed Description
pg_analyze_and_rewrite_varparams is similar to pg_analyze_and_rewrite_fixedparams but designed for scenarios where parameter types are not predetermined and can be inferred from the query context. This function is particularly important for prepared statements where the client doesn't specify parameter types explicitly, allowing PostgreSQL to deduce the appropriate types based on how parameters are used within the query.

The function follows the same two-stage process as its fixed-parameter counterpart: parse analysis followed by rule rewriting. However, it uses parse_analyze_varparams() which can infer parameter types from context, such as from comparison operations, function calls, or assignment contexts. After analysis, the function validates that all parameter types were successfully determined, throwing an error if any parameter remains indeterminate (InvalidOid or UNKNOWNOID).

This flexibility makes the function essential for PostgreSQL's extended query protocol, where clients can prepare statements without explicitly specifying parameter types, relying on the server's type inference capabilities.

## Parameters / Member Variables
- `parsetree`: Raw parse tree (RawStmt) from the grammar parser to be analyzed and rewritten
- `query_string`: Original SQL query string for error reporting and logging purposes
- `paramTypes`: Pointer to array of parameter type OIDs (modified by the function to return inferred types)
- `numParams`: Pointer to number of parameters (may be modified during analysis)
- `queryEnv`: Query environment providing context for query processing (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [parse_analyze_varparams](parse_analyze_varparams.md)
  - [pg_rewrite_query](pg_rewrite_query.md)
  - [ResetUsage](../R/ResetUsage.md)
  - [ShowUsage](../S/ShowUsage.md)
  - InvalidOid (constant)
  - UNKNOWNOID (constant)
  - [RawStmt](../R/RawStmt.md) (type)
  - [QueryEnvironment](../Q/QueryEnvironment.md) (type)
- Called from (representative examples):
  - [PrepareQuery](../P/PrepareQuery.md)
  - [exec_parse_message](../e/exec_parse_message.md)

## Notes and Other Information
- Returns a List of Query nodes since rewriting may expand one query into several
- Modifies paramTypes and numParams arrays to return inferred parameter information
- Essential for PostgreSQL's extended query protocol and prepared statement flexibility
- Validates that all parameter types are successfully determined, preventing runtime errors
- Cannot be executed in aborted transactions due to catalog access requirements
- Supports performance monitoring and tracing capabilities
- Critical for maintaining type safety while providing maximum flexibility in parameter usage
- More complex than the fixed-parameter variant due to type inference requirements

## Simplified Source

```c
// Simplified version of pg_analyze_and_rewrite_varparams
List *
pg_analyze_and_rewrite_varparams(RawStmt *parsetree,
                                const char *query_string,
                                Oid **paramTypes,
                                int *numParams,
                                QueryEnvironment *queryEnv)
{
    Query *query;
    List *querytree_list;

    // Step 1: Parse analysis with parameter type inference
    query = parse_analyze_varparams(parsetree, query_string,
                                   paramTypes, numParams, queryEnv);

    // Step 2: Validate all parameter types were determined
    for (int i = 0; i < *numParams; i++) {
        Oid ptype = (*paramTypes)[i];

        if (ptype == InvalidOid || ptype == UNKNOWNOID) {
            ereport(ERROR,
                   (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                    errmsg("could not determine data type of parameter $%d",
                           i + 1)));
        }
    }

    // Step 3: Apply rewrite rules to transform the query
    querytree_list = pg_rewrite_query(query);

    return querytree_list;
}
```

Key simplifications made:
- Removed performance tracing and logging code for clarity
- Consolidated error handling to focus on the essential validation
- Maintained the core two-phase process: parse analysis then rewriting
- Preserved parameter type validation which is critical for correctness
- Kept the essential error reporting for indeterminate parameter types