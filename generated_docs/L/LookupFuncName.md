# LookupFuncName

## Location
[src/backend/parser/parse_func.c:2144-2205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L2144-L2205)

## Overview
LookupFuncName provides a high-level interface for looking up functions by name and argument types, handling error reporting and supporting the missing_ok parameter for optional existence checking.

## Definition
```c
Oid LookupFuncName(List *funcname, int nargs, const Oid *argtypes, bool missing_ok)
```

## Detailed Description
This function serves as the primary public interface for function name lookup in PostgreSQL's parser. It wraps LookupFuncNameInternal with OBJECT_FUNCTION as the object type, ensuring only functions (not procedures) are returned. The function handles schema-qualified and unqualified names, searching through the current namespace search path for unqualified names. When lookups fail, it generates appropriate error messages that distinguish between 'function not found' and 'ambiguous function name' scenarios, providing helpful hints to users about specifying argument lists for disambiguation.

## Parameters / Member Variables
- `funcname`: List containing the possibly schema-qualified function name parts
- `nargs`: Number of arguments (-1 indicates unspecified argument count/types)  
- `argtypes`: Array of argument type OIDs (can be NULL if nargs == 0)
- `missing_ok`: If true, returns InvalidOid instead of throwing error when function not found

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncNameInternal](LookupFuncNameInternal.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errhint](../e/errhint.md)
  - [NameListToString](../N/NameListToString.md)
  - [func_signature_string](../f/func_signature_string.md)
  - ERRCODE_UNDEFINED_FUNCTION
  - ERRCODE_AMBIGUOUS_FUNCTION
- Called from (representative examples):
  - Various parser functions for function call resolution
  - SQL statement processing functions

## Notes and Other Information
This function specifically excludes procedures from search results, even if they match the name and arguments. For historical compatibility, it allows aggregates and window functions to be found when searching for functions. The function provides user-friendly error messages with suggestions for resolving ambiguous function names by specifying argument lists. When nargs is -1, it indicates that argument types are not specified, which can lead to ambiguity errors if multiple functions exist with the same name.