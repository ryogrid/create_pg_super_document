# pg_get_functiondef

## Location
[src/backend/utils/adt/ruleutils.c:2881-3132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2881-L3132)

## Overview
Generates the complete "CREATE OR REPLACE FUNCTION/PROCEDURE" SQL statement for a specified function or procedure, including all attributes and the function body.

## Definition
```c
Datum pg_get_functiondef(PG_FUNCTION_ARGS)
```

## Detailed Description
pg_get_functiondef is a comprehensive PostgreSQL system function that reconstructs the complete DDL statement for creating a function or procedure. It takes a function OID as input and produces a complete "CREATE OR REPLACE FUNCTION" or "CREATE OR REPLACE PROCEDURE" statement that could be used to recreate the function with all its attributes and settings.

The function performs extensive formatting and handles numerous function attributes including: function/procedure type, parameters, return types, language, volatility, parallel safety, strictness, security definer status, leakproof attribute, cost and rows estimates, support functions, configuration parameters, and the function body itself. For SQL language functions, it handles special formatting for SQL body syntax, while for other languages it uses dollar quoting with conflict-avoiding delimiters.

The output is designed to be compatible with psql's \ef and \sf commands, which rely on specific formatting patterns to identify the start of the function body (lines beginning with "AS ", "BEGIN ", or "RETURN ").

## Parameters / Member Variables
- `funcid`: OID of the function or procedure whose definition should be generated

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (macro for extracting OID argument)
  - [initStringInfo](../i/initStringInfo.md) (initializes StringInfo buffer)
  - [SearchSysCache1](../S/SearchSysCache1.md) (looks up function in pg_proc)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (converts OID to Datum)
  - HeapTupleIsValid (validates tuple lookup result)
  - GETSTRUCT (extracts structure from heap tuple)
  - [get_namespace_name_or_temp](../g/get_namespace_name_or_temp.md) (gets namespace name for function)
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md) (quotes schema.function name)
  - [print_function_arguments](print_function_arguments.md) (formats function parameter list)
  - [print_function_rettype](print_function_rettype.md) (formats return type specification)
  - [print_function_trftypes](print_function_trftypes.md) (formats transform types)
  - [get_language_name](../g/get_language_name.md) (gets language name from OID)
  - [quote_identifier](../q/quote_identifier.md) (quotes SQL identifier)
  - [generate_function_name](../g/generate_function_name.md) (creates qualified function name)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (extracts attributes from system cache)
  - DatumGetArrayTypeP (converts Datum to ArrayType)
  - [array_ref](../a/array_ref.md) (extracts array element)
  - TextDatumGetCString (converts TEXT datum to C string)
  - [GetConfigOptionFlags](../G/GetConfigOptionFlags.md) (gets GUC option flags)
  - [SplitGUCList](../S/SplitGUCList.md) (parses GUC list value)
  - [simple_quote_literal](../s/simple_quote_literal.md) (quotes string literal for SQL)
  - [print_function_sqlbody](print_function_sqlbody.md) (formats SQL function body)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (appends binary data to StringInfo)
  - [string_to_text](../s/string_to_text.md) (converts C string to TEXT)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases system cache entry)
  - PG_RETURN_TEXT_P (macro for returning TEXT result)
- Called from:
  - SQL function pg_get_functiondef() available to users
  - PostgreSQL system utilities and tools

## Notes and Other Information
- This function is exposed as a SQL-callable system function in PostgreSQL
- Returns NULL if the function OID does not exist or is not accessible
- Rejects aggregate functions with an appropriate error message
- Handles both functions and procedures with appropriate CREATE syntax
- Uses dollar quoting with conflict-avoiding delimiters ($ or $ extended as needed)
- Formats configuration parameters (SET clauses) with proper SQL quoting
- Handles special cases for internal/C language functions vs others for default cost values
- Output format is designed to be compatible with psql's function editing commands
- Always qualifies function names to ensure correct replacement during CREATE OR REPLACE
- Located in src/backend/utils/adt/ruleutils.c:2881-3132
- Extensive attribute handling ensures complete function recreation capability

## Simplified Source

```c
Datum
pg_get_functiondef(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);
    StringInfoData buf;
    HeapTuple proctup;
    Form_pg_proc proc;
    bool isfunction;
    const char *name, *nsp;

    initStringInfo(&buf);

    // Look up the function in system catalog
    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(proctup))
        PG_RETURN_NULL();

    proc = (Form_pg_proc) GETSTRUCT(proctup);
    name = NameStr(proc->proname);

    // Reject aggregate functions
    if (proc->prokind == PROKIND_AGGREGATE)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is an aggregate function", name)));

    isfunction = (proc->prokind != PROKIND_PROCEDURE);

    // Generate CREATE OR REPLACE FUNCTION/PROCEDURE statement
    nsp = get_namespace_name_or_temp(proc->pronamespace);
    appendStringInfo(&buf, "CREATE OR REPLACE %s %s(",
                     isfunction ? "FUNCTION" : "PROCEDURE",
                     quote_qualified_identifier(nsp, name));

    // Add function arguments
    print_function_arguments(&buf, proctup, false, true);
    appendStringInfoString(&buf, ")\n");

    // Add return type for functions
    if (isfunction)
    {
        appendStringInfoString(&buf, " RETURNS ");
        print_function_rettype(&buf, proctup);
        appendStringInfoChar(&buf, '\n');
    }

    // Add function properties (language, volatility, etc.)
    appendStringInfo(&buf, " LANGUAGE %s\n",
                     quote_identifier(get_language_name(proc->prolang, false)));

    // Add various function attributes
    if (proc->prokind == PROKIND_WINDOW)
        appendStringInfoString(&buf, " WINDOW");

    switch (proc->provolatile)
    {
        case PROVOLATILE_IMMUTABLE:
            appendStringInfoString(&buf, " IMMUTABLE");
            break;
        case PROVOLATILE_STABLE:
            appendStringInfoString(&buf, " STABLE");
            break;
    }

    if (proc->proisstrict)
        appendStringInfoString(&buf, " STRICT");
    if (proc->prosecdef)
        appendStringInfoString(&buf, " SECURITY DEFINER");

    // Add function body with appropriate quoting
    if (proc->prolang == SQLlanguageId)
    {
        print_function_sqlbody(&buf, proctup);
    }
    else
    {
        appendStringInfoString(&buf, "AS ");
        // Add function source with dollar quoting
        Datum tmp = SysCacheGetAttrNotNull(PROCOID, proctup, Anum_pg_proc_prosrc);
        const char *prosrc = TextDatumGetCString(tmp);

        StringInfoData dq;
        initStringInfo(&dq);
        appendStringInfoString(&dq, isfunction ? "$function$" : "$procedure$");

        appendBinaryStringInfo(&buf, dq.data, dq.len);
        appendStringInfoString(&buf, prosrc);
        appendBinaryStringInfo(&buf, dq.data, dq.len);
    }

    appendStringInfoChar(&buf, '\n');
    ReleaseSysCache(proctup);

    PG_RETURN_TEXT_P(string_to_text(buf.data));
}
```