# format_function_signature

## Location
[src/bin/pg_dump/pg_dump.c:12283-12311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L12283-L12311)

## Overview
Generates a formatted function signature consisting of the function name and argument list, primarily used for referencing functions in pg_dump output.

## Definition
```c
static char *format_function_signature(Archive *fout, const FuncInfo *finfo, bool honor_quotes)
```

## Detailed Description
This function creates a string representation of a function signature in the format "function_name(arg_type1, arg_type2, ...)". It generates only a minimal list of input argument types, which is sufficient to reference the function but not to define it. The function is used extensively throughout pg_dump to create consistent function references in SQL output and TOC entries.

The function uses PostgreSQL's PQExpBuffer system for efficient string building and handles proper formatting of type names through the getFormattedTypeName utility.

## Parameters / Member Variables
- `fout`: Archive pointer containing dump context and formatting information
- `finfo`: FuncInfo structure containing function metadata including name, argument count, and argument types
- `honor_quotes`: Boolean flag determining whether the function name should be quoted using fmtId (true for SQL commands, false for TOC tags)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [fmtId](fmtId.md)
  - [getFormattedTypeName](../g/getFormattedTypeName.md)
- Called from (representative examples):
  - [dumpFunc](../d/dumpFunc.md)
  - [dumpCast](../d/dumpCast.md)
  - [dumpTransform](../d/dumpTransform.md)
  - [dumpAgg](../d/dumpAgg.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- The function is static to pg_dump.c and serves as a utility function for consistent function signature formatting
- When honor_quotes is false, the function name is never quoted, making it suitable for TOC tags but not SQL commands
- The function handles variable argument counts through the FuncInfo structure's nargs field and argtypes array
- Memory management is handled by the PQExpBuffer system, with the caller responsible for freeing the returned string data

## Simplified Source

```c
static char *
format_function_signature(Archive *fout, const FuncInfo *finfo, bool honor_quotes)
{
    PQExpBufferData fn;
    int j;

    initPQExpBuffer(&fn);

    // Add function name with optional quoting
    if (honor_quotes)
        appendPQExpBuffer(&fn, "%s(", fmtId(finfo->dobj.name));
    else
        appendPQExpBuffer(&fn, "%s(", finfo->dobj.name);

    // Add argument types
    for (j = 0; j < finfo->nargs; j++) {
        if (j > 0)
            appendPQExpBufferStr(&fn, ", ");

        appendPQExpBufferStr(&fn,
                             getFormattedTypeName(fout, finfo->argtypes[j], zeroIsError));
    }

    appendPQExpBufferChar(&fn, ')');
    return fn.data;
}
```