# list_member

## Location
[src/backend/nodes/list.c:661-681](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L661-L681)

## Overview
The  function performs a linear search to determine if a given datum is present in a pointer list, using deep equality comparison.

## Definition


## Detailed Description
This function searches through a pointer list to determine whether the specified datum is present as a member. It performs a linear search from the beginning of the list, comparing each element with the provided datum using PostgreSQL's  function, which performs deep structural comparison rather than simple pointer equality.

The function is specifically designed for pointer lists and includes assertions to verify this requirement. It uses the  macro to iterate through the list cells and the  macro to access the pointer value in each cell. The search terminates as soon as a match is found, returning true, or continues through the entire list before returning false if no match is found.

Due to its linear search nature, this function has O(n) time complexity and should be avoided for frequent searches on long lists where performance is critical.

## Parameters / Member Variables
- : The pointer list to search in (marked const as it's not modified)
- : The data element to search for (should be a Node for proper equality comparison)

## Dependencies
- Functions called/Symbols referenced:
  -  - Validates that the list contains pointers
  -  - Validates list consistency before search
  -  - Performs deep equality comparison between elements

- Called from (representative examples):
  - , ,  (src/backend/nodes/list.c:1077-1250)
  - ,  (src/backend/nodes/list.c:1345-1414)
  -  (src/backend/optimizer/path/equivclass.c:882)
  -  (src/backend/optimizer/path/joinpath.c:501)
  -  (src/backend/parser/parse_cte.c:469-525)

## Notes and Other Information
- Only works with pointer lists, not integer or OID lists
- Uses deep equality comparison via , not simple pointer comparison
- Has O(n) time complexity - avoid for performance-critical operations on long lists  
- The datum parameter should be a PostgreSQL Node structure for proper equality testing
- Commonly used as a building block for other list operations like unique append and set operations
- Essential for implementing set-like operations and duplicate detection in PostgreSQL's list-based data structures
- Widely used throughout query parsing, optimization, and catalog operations where membership testing is needed