# print_function_sqlbody

## Location
[src/backend/utils/adt/ruleutils.c:3510-3563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3510-L3563)

## Overview
A static helper function that appends the formatted SQL body of a function to a string buffer, handling both single statements and atomic blocks.

## Definition
```c
static void print_function_sqlbody(StringInfo buf, HeapTuple proctup)
```

## Detailed Description
This function extracts and formats the SQL body of a PostgreSQL function from its prosqlbody attribute. It handles two cases: functions with single SQL statements and functions with multiple statements wrapped in atomic blocks. For multi-statement functions, it formats them as "BEGIN ATOMIC ... END" blocks with proper indentation. The function sets up a deparse namespace context with function name and argument information to properly resolve references within the SQL body, and acquires necessary locks on referenced relations before deparsing queries.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted SQL body will be appended
- `proctup`: HeapTuple containing the function's metadata from the pg_proc system catalog

## Dependencies
- Functions called/Symbols referenced:
  - deparse_namespace
  - Form_pg_proc
  - [get_func_arg_info](../g/get_func_arg_info.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [stringToNode](../s/stringToNode.md)
  - TextDatumGetCString
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md) (called twice)
  - [get_query_def](../g/get_query_def.md) (called twice)
  - PRETTYFLAG_INDENT
  - WRAP_COLUMN_DEFAULT
- Called from (representative examples):
  - [pg_get_functiondef](pg_get_functiondef.md)
  - [pg_get_function_sqlbody](pg_get_function_sqlbody.md)

## Notes and Other Information
- The function distinguishes between single Query nodes and List nodes containing multiple statements
- For atomic blocks, statements are formatted with indentation and semicolon separators
- Acquires at least AccessShareLock on relations referenced in the queries for safety
- Uses different formatting flags for single statements vs. atomic blocks (no indentation for single statements)
- The deparse namespace is populated with function name and argument names to enable proper variable resolution
- This function is essential for reconstructing CREATE FUNCTION statements with SQL function bodies

## Simplified Source

```c
static void print_function_sqlbody(StringInfo buf, HeapTuple proctup) {
    int numargs;
    Oid *argtypes;
    char **argnames;
    char *argmodes;
    deparse_namespace dpns = {0};
    Datum tmp;
    Node *n;

    // Set up deparse namespace with function info
    dpns.funcname = pstrdup(NameStr(((Form_pg_proc) GETSTRUCT(proctup))->proname));
    numargs = get_func_arg_info(proctup, &argtypes, &argnames, &argmodes);
    dpns.numargs = numargs;
    dpns.argnames = argnames;

    // Get the function's SQL body
    tmp = SysCacheGetAttrNotNull(PROCOID, proctup, Anum_pg_proc_prosqlbody);
    n = stringToNode(TextDatumGetCString(tmp));

    if (IsA(n, List)) {
        // Multiple statements - format as atomic block
        List *stmts = linitial(castNode(List, n));
        ListCell *lc;

        appendStringInfoString(buf, "BEGIN ATOMIC\n");

        foreach(lc, stmts) {
            Query *query = lfirst_node(Query, lc);

            // Lock relations and deparse the query
            AcquireRewriteLocks(query, false, false);
            get_query_def(query, buf, list_make1(&dpns), NULL, false,
                         PRETTYFLAG_INDENT, WRAP_COLUMN_DEFAULT, 1);
            appendStringInfoChar(buf, ';');
            appendStringInfoChar(buf, '\n');
        }

        appendStringInfoString(buf, "END");
    } else {
        // Single statement
        Query *query = castNode(Query, n);

        // Lock relations and deparse the query
        AcquireRewriteLocks(query, false, false);
        get_query_def(query, buf, list_make1(&dpns), NULL, false,
                     0, WRAP_COLUMN_DEFAULT, 0);
    }
}
```