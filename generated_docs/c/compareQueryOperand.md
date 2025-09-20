# compareQueryOperand

## Location
[src/backend/utils/adt/tsrank.c:135-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L135-L153)

## Overview
A comparison function used for sorting QueryOperand pointers by their string content, following the standard qsort comparison interface.

## Definition

```c
static int
compareQueryOperand(const void *a, const void *b, void *arg)
```
## Detailed Description
The  function serves as a comparison callback for sorting arrays of QueryOperand pointers. It extracts the string content from two QueryOperand structures and uses PostgreSQL's  function to perform lexicographical comparison. The function is designed to be used with sorting functions like  that require a three-parameter comparison function.

The function dereferences the void pointers to get QueryOperand pointers, then uses the operand string (passed as the arg parameter) along with each QueryOperand's distance and length fields to locate and compare the actual string content. This enables sorting of query operands in lexicographical order, which is useful for deduplication and efficient processing of text search queries.

## Parameters / Member Variables
- : Pointer to the first QueryOperand pointer to compare
- : Pointer to the second QueryOperand pointer to compare  
- : Pointer to the operand string buffer containing all query strings

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL's string comparison function)
  -  (query operand structure)
- Called from (representative examples):
  -  (src/backend/utils/adt/tsrank.c:179, 187)

## Notes and Other Information
- This is a static function, accessible only within tsrank.c
- Follows the standard three-parameter comparison function signature used by 
- Returns negative, zero, or positive integer based on the comparison result (same as strcmp)
- The comparison is case-sensitive as indicated by the false parameter passed to 
- Used primarily for sorting QueryOperand arrays to enable efficient deduplication and processing
- The arg parameter provides access to the string buffer where actual operand text is stored
- Essential component of query preprocessing in PostgreSQL's text search ranking system