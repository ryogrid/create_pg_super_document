# index_can_return

## Location
src/backend/access/index/indexam.c: 788 - 825

## Overview
Determines whether an index access method supports index-only scans for a given column, enabling query optimization decisions for covering indexes.

## Definition
```c
bool index_can_return(Relation indexRelation, int attno)
```

## Detailed Description
The `index_can_return` function is a critical component of PostgreSQL's query optimization system, specifically for enabling index-only scans (also known as covering index scans). This function determines whether a particular index can return the value of a specified column without needing to access the heap table.

Index-only scans are a powerful optimization where the query executor can satisfy a query entirely from index data, avoiding expensive heap tuple fetches. This is particularly beneficial for queries where:
- The index contains all columns needed by the query
- The visibility information can be determined from the visibility map
- The access method supports returning column values

The function checks if the access method provides an `amcanreturn` procedure and, if so, delegates the decision to that method-specific function. If no `amcanreturn` procedure is provided, the function conservatively returns false, assuming the index cannot support index-only scans.

## Parameters / Member Variables
- `indexRelation`: Relation structure representing the index to check
- `attno`: Attribute number (column number) to check for returnability

## Dependencies
- Functions called/Symbols referenced:
  - RELATION_CHECKS (macro for relation validation)
  - amcanreturn (access method specific procedure for determining column returnability)
- Called from (representative examples):
  - [get_relation_info](../g/get_relation_info.md) (relation information gathering for query planning)
  - [indexam_property](indexam_property.md) (index access method property queries)
  - IndexScanIsValid (index scan validation)

## Notes and Other Information
- The `amcanreturn` procedure is optional for access methods; if not provided, the function assumes false
- Essential for query planner decisions about whether to use index-only scans
- Different index types have different capabilities: B-tree indexes can typically return all indexed columns, while some specialized indexes may have limitations
- Used during query planning to determine the most efficient execution strategy
- The attribute number follows PostgreSQL's convention where system columns have negative numbers and user columns start from 1
- Part of the broader index-only scan infrastructure that can significantly improve query performance by avoiding heap access