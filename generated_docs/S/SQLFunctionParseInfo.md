# SQLFunctionParseInfo

## Location
[src/include/executor/functions.h:25-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/functions.h#L25-L33)

## Overview
SQLFunctionParseInfo is a data structure used by parser callback hooks to resolve parameter references during parsing of a SQL function's body, providing essential metadata about function arguments and their types.

## Definition

```c
typedef struct SQLFunctionParseInfo
{
	char	   *fname;			/* function's name */
	int			nargs;			/* number of input arguments */
	Oid		   *argtypes;		/* resolved types of input arguments */
	char	  **argnames;		/* names of input arguments; NULL if none */
	/* Note that argnames[i] can be NULL, if some args are unnamed */
	Oid			collation;		/* function's input collation, if known */
} SQLFunctionParseInfo;
```
## Detailed Description
The SQLFunctionParseInfo structure serves as a critical component in PostgreSQL's SQL function parsing infrastructure. It is specifically designed to be separate from SQLFunctionCache to support scenarios where parsing and execution occur independently. This structure provides the parser with essential information needed to resolve parameter references (, , etc.) within SQL function bodies by maintaining metadata about the function's signature, argument types, and names.

The structure is primarily used during the CREATE FUNCTION process for SQL functions and during function execution when parsing is required. It enables the parser to validate parameter references and perform proper type resolution, ensuring that SQL function bodies correctly reference their declared parameters.

## Parameters / Member Variables
- `*fname`: The function's name, used to qualify argument names during parsing and for error reporting
- `nargs`: The total number of input arguments the function accepts
- `*argtypes`: An array of OIDs representing the resolved data types of input arguments, with polymorphic types resolved to their actual types
- `**argnames`: An array of argument names; can be NULL if no argument names are provided, and individual elements can be NULL for unnamed arguments
- `collation`: The input collation for the function, used for proper string comparison and sorting operations
## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - Standard memory allocation functions (palloc)

- Called from (representative examples):
  - [interpret_AS_clause](../i/interpret_AS_clause.md) (src/backend/commands/functioncmds.c:899)
  - [prepare_sql_fn_parse_info](../p/prepare_sql_fn_parse_info.md) (src/backend/executor/functions.c:184)
  - SQLFunctionParseInfoPtr typedef (src/include/executor/functions.h:35)

## Notes and Other Information
- The structure is allocated separately from SQLFunctionCache to support independent parsing and execution phases
- Polymorphic argument types are resolved to their actual types before being stored in the argtypes array
- The argnames array handles both completely unnamed argument lists (NULL array) and partially named lists (some NULL elements)
- This structure is essential for enabling parameter reference resolution (, , etc.) in SQL function bodies
- Used primarily during CREATE FUNCTION processing for SQL language functions and during function execution when re-parsing is required
- The collation field supports proper handling of collation-sensitive operations within SQL function bodies