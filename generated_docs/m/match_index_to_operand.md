# match_index_to_operand

## Location
[src/backend/optimizer/path/indxpath.c:3665-3750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L3665-L3750)

## Overview
Tests for a match between an index's key and the operand on one side of a restriction or join clause, used to determine if an index can be utilized for a given query condition.

## Definition
```c
bool match_index_to_operand(Node *operand, int indexcol, IndexOptInfo *index)
```

## Detailed Description
This function performs a generalized test to determine if a given operand (from a WHERE clause or join condition) matches a specific column of an index. It handles both simple index columns (direct table attributes) and expression-based index columns. The function strips RelabelType nodes for binary-compatible operator cases and performs deep equality checks for expression indexes. This is a core function in PostgreSQL's index selection logic during query optimization.

## Parameters / Member Variables
- `operand`: The nodetree to be compared to the index (typically from a WHERE clause condition)
- `indexcol`: The column number of the index (counting from 0) to test against  
- `index`: The IndexOptInfo structure containing details about the index of interest

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for node type checking)
  - [list_head](../l/list_head.md) (list traversal function)
  - [lnext](../l/lnext.md) (list navigation function) 
  - [equal](../e/equal.md) (deep equality comparison for expression trees)
  - elog (error logging function)
- Called from (representative examples):
  - [match_clause_to_indexcol](match_clause_to_indexcol.md) (src/backend/optimizer/path/indxpath.c:2256)
  - [match_boolean_index_clause](match_boolean_index_clause.md) (src/backend/optimizer/path/indxpath.c:2314)
  - [match_opclause_to_indexcol](match_opclause_to_indexcol.md) (src/backend/optimizer/path/indxpath.c:2433)
  - [match_funcclause_to_indexcol](match_funcclause_to_indexcol.md) (src/backend/optimizer/path/indxpath.c:2535)
  - [get_actual_variable_range](../g/get_actual_variable_range.md) (src/backend/utils/adt/selfuncs.c:6203)

## Notes and Other Information
- The function is exported for use in selfuncs.c for selectivity estimation
- Collations are not checked by this function; the caller must verify collation compatibility separately
- [RelabelType](../R/RelabelType.md) nodes are automatically stripped to handle binary-compatible operator cases
- For expression indexes, the function searches through the index's expression list to find the matching expression
- Returns false if no match is found, true if the operand matches the specified index column
- Critical for determining index usability during query planning and optimization