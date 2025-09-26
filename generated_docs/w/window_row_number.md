# window_row_number

## Location
[src/backend/utils/adt/windowfuncs.c:84-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/windowfuncs.c#L84-L97)

## Overview
Implements the ROW_NUMBER() window function, which assigns a sequential integer to each row within a partition, starting from 1.

## Definition
```c
Datum window_row_number(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL ROW_NUMBER() window function, which returns a unique sequential integer for each row within its partition. The numbering starts at 1 for the first row and increments by 1 for each subsequent row. Unlike ranking functions like RANK() or DENSE_RANK(), ROW_NUMBER() does not consider the values in the ORDER BY clause - it simply assigns sequential numbers based on the physical order of rows within the partition.

The function is straightforward: it gets the current position within the partition (0-based) and returns that position plus 1 to make it 1-based numbering. It also sets the mark position for proper window function processing.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_WINDOW_OBJECT
  - [WinGetCurrentPosition](../W/WinGetCurrentPosition.md)
  - [WinSetMarkPosition](../W/WinSetMarkPosition.md)
  - PG_RETURN_INT64
  - [WindowObject](../W/WindowObject.md) (type)
- Called from (representative examples):
  - This is a PostgreSQL built-in function called directly from SQL queries

## Notes and Other Information
- Returns a 64-bit integer (int64) to handle large result sets
- The function is registered in PostgreSQL's system catalogs and can be called from SQL as ROW_NUMBER()
- Unlike ranking functions, ROW_NUMBER() always produces unique values within each partition
- The mark position is set to the current position to maintain proper window function state
- This function does not use any persistent context between calls within a partition