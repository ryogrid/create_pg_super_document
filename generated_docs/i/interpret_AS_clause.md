# interpret_AS_clause

## Location
[src/backend/commands/functioncmds.c:851-1010](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L851-L1010)

## Overview
Processes and validates the AS clause of CREATE FUNCTION/PROCEDURE statements, handling different function body formats based on the programming language.

## Definition
```c
static void interpret_AS_clause(Oid languageOid, const char *languageName,
                               char *funcname, List *as, Node *sql_body_in,
                               List *parameterTypes, List *inParameterNames,
                               char **prosrc_str_p, char **probin_str_p,
                               Node **sql_body_out,
                               const char *queryString)
```

## Detailed Description
This static function interprets the AS clause of function definitions differently based on the target language. For C language functions, it handles object file names and optional link symbol names. For SQL functions with unquoted bodies, it performs comprehensive parsing and transformation of the SQL statements. For other languages, it stores the function body as a string.

The function performs extensive validation including checking for duplicate or missing function bodies, ensuring SQL bodies are only used with SQL language, validating polymorphic argument restrictions, and transforming SQL statements through the parser. It sets up specialized parsing context for SQL functions using sql_fn_parser_setup().

## Parameters / Member Variables
- `languageOid`: OID of the function's programming language
- `languageName`: String name of the programming language
- `funcname`: Name of the function being created
- `as`: List containing AS clause elements (file names, function bodies, etc.)
- `sql_body_in`: Input SQL body node for unquoted SQL functions
- `parameterTypes`: List of parameter type OIDs
- `inParameterNames`: List of parameter names
- `prosrc_str_p`: Output pointer for function source code string
- `probin_str_p`: Output pointer for binary/object file path
- `sql_body_out`: Output pointer for processed SQL body node
- `queryString`: Original CREATE FUNCTION query text for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - strVal (extracts string values from nodes)
  - linitial, lsecond (list access functions)
  - [list_length](../l/list_length.md), list_nth, list_nth_oid (list utility functions)
  - [SQLFunctionParseInfo](../S/SQLFunctionParseInfo.md) (structure type for SQL function parsing)
  - IsPolymorphicType (checks if type is polymorphic)
  - [make_parsestate](../m/make_parsestate.md), free_parsestate (parser state management)
  - [sql_fn_parser_setup](../s/sql_fn_parser_setup.md) (sets up SQL function parsing context)
  - [transformStmt](../t/transformStmt.md) (transforms parsed statements)
  - [GetCommandTagName](../G/GetCommandTagName.md), CreateCommandTag (command type utilities)
  - [pstrdup](../p/pstrdup.md) (string duplication)
- Called from (representative examples):
  - [CreateFunction](../C/CreateFunction.md) (src/backend/commands/functioncmds.c:1222)

## Notes and Other Information
- Handles three distinct cases: C language (object files), SQL language with unquoted bodies, and other languages (string bodies)
- For C functions, supports both explicit link symbol names and automatic function name substitution
- Maintains backward compatibility with PostgreSQL versions before 8.4 by handling "-" as omitted link symbol
- For SQL functions, validates against polymorphic arguments and utility statements in unquoted bodies
- Provides comprehensive error reporting with appropriate error codes and messages
- Sets up proper parsing context for SQL functions to handle parameter references correctly
- Part of PostgreSQL's multi-language function definition system supporting C, SQL, PL/pgSQL, and other procedural languages