# circle_center

## Location
[src/backend/utils/adt/geo_ops.c:5143-5158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5143-L5158)

## Overview
Returns the center point of a circle as a Point data type.

## Definition

```c
Datum
circle_center(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function extracts the center point from a PostgreSQL CIRCLE geometric data type. It takes a circle as input and returns a newly allocated Point structure containing the x and y coordinates of the circle's center. This function is part of PostgreSQL's geometric operations and is typically used in SQL queries to retrieve the center point of circle objects.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Input CIRCLE structure accessed via 

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts CIRCLE argument from function args
  -  - PostgreSQL memory allocation function
  -  - Returns Point result to PostgreSQL
- Data types used:
  -  - Input circle structure
  -  - Output point structure
  -  - PostgreSQL function return type
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function allocates memory for the result Point using , which is PostgreSQL's memory management system
- The center coordinates are directly copied from the input circle's center field
- This is a PostgreSQL built-in function that can be called from SQL as 
- Memory allocation is handled by PostgreSQL's memory context system