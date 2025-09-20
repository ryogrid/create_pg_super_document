# cstring_in

## Location
[src/backend/utils/adt/pseudotypes.c:107-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pseudotypes.c#L107-L114)

## Overview
The  function is an input conversion function for the  pseudo-type in PostgreSQL, converting a C-style null-terminated string into PostgreSQL's internal cstring representation.

## Definition

```c
Datum
cstring_in(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the input conversion function for PostgreSQL's  pseudo-type. Although  is marked as a pseudo-type to prevent its use in table definitions, it provides a complete set of I/O functions for internal use and manual invocation of datatype I/O functions. This function takes a C-style null-terminated string as input and returns a PostgreSQL  by duplicating the input string using  to ensure proper memory management within PostgreSQL's memory context system.

## Parameters / Member Variables
- The function follows PostgreSQL's standard function calling convention using , which provides access to:
  - Input parameter: A C-style null-terminated string obtained via 

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for extracting cstring argument)
  -  (PostgreSQL memory-managed string duplication)
  -  (macro for returning cstring result)
- Called from (representative examples):
  - Manual invocation in SQL queries like 
  - Internal type system operations

## Notes and Other Information
- The  type is specifically marked as a pseudo-type to discourage its use in table definitions
- Despite being a pseudo-type, it provides full I/O functionality to support manual datatype I/O function invocations
- The function ensures proper memory management by using  instead of direct string copying
- Located in 