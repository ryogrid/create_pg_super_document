# pg_ts_template_is_visible

## Location
[src/backend/catalog/namespace.c:5048-5061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L5048-L5061)

## Overview
A PostgreSQL system function that determines whether a text search template is visible in the current search path, returning NULL if the template does not exist.

## Definition

```c
Datum
pg_ts_template_is_visible(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL system function that checks the visibility of a text search template within the current schema search path. It takes a template OID as input and returns a boolean value indicating whether the template is accessible from the current context. The function handles missing templates gracefully by returning NULL instead of throwing an error, making it suitable for use in SQL queries where the existence of the template is uncertain.

The function leverages the internal  function to perform the actual visibility check, which considers the current namespace search path and handles namespace precedence rules to determine if the specified template would be found when referenced by name.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The object identifier of the text search template to check

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for extracting OID argument)
  -  (internal visibility checking function)
  -  (macro for returning NULL)
  -  (macro for returning boolean result)
- Called from:
  - Available as SQL system function 

## Notes and Other Information
- Returns boolean  if the template is visible,  if not visible, or  if the template doesn't exist
- The visibility determination follows PostgreSQL's standard namespace search path resolution
- This function is part of PostgreSQL's text search infrastructure
- Located in 
- The function uses the "Ext" variant of the visibility checker to handle missing objects gracefully
- Similar in structure and purpose to  but operates on text search templates instead of dictionaries