# ExecGetReturningSlot

## Location
src/backend/executor/execUtils.c: 1182 - 1205

## Overview
Returns a tuple slot for processing RETURNING clause results in INSERT, UPDATE, and DELETE statements, creating it lazily if it doesn't already exist.

## Definition


## Detailed Description
This function provides access to a specialized tuple slot used for processing RETURNING clauses in data modification statements (INSERT, UPDATE, DELETE). The RETURNING clause allows these statements to return data from the rows that were inserted, updated, or deleted, making them more powerful than traditional DML statements that only return row counts.

Like other similar functions in the executor utilities, it implements lazy initialization - the slot is created only when first needed and cached in the ResultRelInfo structure for subsequent operations within the same query execution.

The slot is essential for storing and formatting the result tuples that will be returned to the client when a RETURNING clause is specified. It ensures that the returned data has the correct structure and type information matching the target relation.

## Parameters / Member Variables
- : The executor state containing query execution context and memory management information
- : Result relation info structure that maintains various tuple slots and relation metadata for the target relation

## Dependencies
- Functions called/Symbols referenced:
  -  (creates and initializes a new tuple slot)
  -  (gets appropriate slot callback functions for the table)
  -  (referenced in the function context)
- Called from (representative examples):
  -  (src/backend/executor/nodeModifyTable.c:1103)
  -  (src/backend/executor/nodeModifyTable.c:1496, 1714)
  -  (src/include/executor/executor.h:616)

## Notes and Other Information
- Uses lazy initialization pattern - slot is only created when first accessed
- The slot is stored in  for reuse within the same query execution
- Memory context is temporarily switched to  to ensure proper lifetime management
- Essential for implementing PostgreSQL's RETURNING clause functionality in DML statements
- Enables statements like  or 
- The slot structure matches the target relation's tuple descriptor to ensure type compatibility
- Part of PostgreSQL's advanced DML capabilities that go beyond standard SQL's row-count-only returns
- Used in conjunction with ModifyTable executor nodes that handle data modification operations