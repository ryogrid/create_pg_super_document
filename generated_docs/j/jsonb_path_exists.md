# jsonb_path_exists

## Location
[src/backend/utils/adt/jsonpath_exec.c:427-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L427-L432)

## Overview
Public PostgreSQL function that checks whether a JSONPath expression returns at least one item for a given JSONB value, without timezone awareness.

## Definition

```c
Datum
jsonb_path_exists(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the public interface for JSONPath existence checking in PostgreSQL. It is a simple wrapper around  that provides timezone-naive JSONPath execution. The function is designed to be called directly from SQL as a built-in PostgreSQL function.

This function implements the core functionality behind the @? operator and is used to determine if a JSONPath expression matches any elements within a JSONB document. It follows PostgreSQL's function call convention using  to access arguments and returns a  result.

## Parameters / Member Variables
The function uses PostgreSQL's standard function argument mechanism:
- Arguments are accessed through the  parameter passed to the internal function
- Argument 0: JSONB document to search within
- Argument 1: JSONPath expression to evaluate  
- Argument 2 (optional): JSONB object containing variables for the JSONPath expression
- Argument 3 (optional): Boolean flag for silent mode operation

## Dependencies
- Functions called/Symbols referenced:
  -  - The internal implementation function

- Called from:
  - This is a public PostgreSQL function, typically called from SQL queries or the function manager system
  - No direct C function references found in the codebase

## Notes and Other Information
- This function is marked as a public -returning function, making it accessible as a PostgreSQL built-in function
- The function passes  as the timezone parameter to , meaning it operates without timezone awareness
- Part of PostgreSQL's JSONPath functionality introduced to support SQL/JSON standard operations
- Used internally to implement the @? operator for JSONB JSONPath existence checking
- The function follows PostgreSQL's standard function calling convention with 
- All actual processing is delegated to  with timezone support disabled