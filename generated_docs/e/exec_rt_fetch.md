# exec_rt_fetch

## Location
src/include/executor/executor.h: 588 - 682

## Overview
exec_rt_fetch retrieves a specific RangeTblEntry from the executor state's range table by index, providing access to table metadata and query information during query execution.

## Definition


## Detailed Description
exec_rt_fetch is a fundamental utility function in PostgreSQL's executor that provides indexed access to range table entries. The range table (es_range_table) in the EState contains metadata about all tables, subqueries, functions, and other relation-like entities referenced in a query. Each entry is identified by a Range Table Index (RTI), which is a 1-based index used throughout the query execution system.

This function performs a simple but critical operation: it converts the 1-based RTI to a 0-based list index and retrieves the corresponding RangeTblEntry from the estate's range table. The RangeTblEntry contains essential information such as the relation OID, alias information, column information, security context, and access permissions needed during query execution.

The function is implemented as a static inline function for optimal performance since it's called frequently throughout query execution whenever executor nodes need to access table metadata.

## Parameters / Member Variables
- : Range Table Index (1-based) identifying which RangeTblEntry to retrieve from the range table
- : EState containing the es_range_table list with all RangeTblEntry objects for the current query

## Dependencies
- Functions called/Symbols referenced:
  - list_nth (for list element access)
- Called from (representative examples):
  - ExecGetRangeTableRelation (relation access)
  - ExecEvalWholeRowVar (whole-row variable evaluation)
  - InitPlan (plan initialization)
  - ExecInitIndexScan (index scan initialization)
  - ExecInitBitmapIndexScan (bitmap index scan initialization)

## Notes and Other Information
- Uses 1-based indexing (RTI) which is converted to 0-based for list_nth access
- The returned RangeTblEntry pointer should not be modified as it's shared across the query execution
- Essential for translating RTI references in plan nodes to actual table metadata
- Range table entries can represent regular tables, subqueries, functions, VALUES clauses, CTEs, and other relation-like constructs
- This function provides the foundation for most table-related operations in the executor
- The inline implementation ensures minimal overhead for this frequently-called function