# ri_PerformCheck

## Location
src/backend/utils/adt/ri_triggers.c: 2312 - 2448

## Overview
Performs a query to enforce a referential integrity restriction by executing a pre-planned SPI query with appropriate snapshots and security contexts.

## Definition


## Detailed Description
This is a core function in PostgreSQL's referential integrity enforcement system. It executes pre-compiled SPI queries to check foreign key constraints, handling various constraint actions like RESTRICT, CASCADE, SET NULL, etc. The function manages transaction snapshots appropriately for different isolation levels, switches to the table owner's security context for permission checks, and extracts key values from tuple slots to use as query parameters.

The function determines whether to query the primary key or foreign key table based on the query type, extracts the appropriate values from the source tuples, manages snapshots for consistency in different isolation levels, and executes the query with proper security context switching.

## Parameters / Member Variables
- : Constraint information structure containing details about the foreign key relationship
- : Query key identifying the specific type of RI query to execute
- : Pre-compiled SPI plan for the query to be executed
- : Foreign key table relation
- : Primary key table relation  
- : Tuple slot containing the old tuple values (for updates/deletes)
- : Tuple slot containing the new tuple values (for inserts/updates)
- : Whether to detect rows that became visible after transaction start
- : Expected SPI result code for validation

## Dependencies
- Functions called/Symbols referenced:
  - ri_ExtractValues
  - ri_ReportViolation
  - IsolationUsesXactSnapshot
  - CommandCounterIncrement
  - GetLatestSnapshot
  - GetTransactionSnapshot
  - GetUserIdAndSecContext
  - SetUserIdAndSecContext
  - RelationGetForm
  - SPI_execute_snapshot
  - SPI_result_code_string
- Called from (representative examples):
  - ri_Check_Pk_Match
  - ri_restrict
  - RI_FKey_cascade_del
  - RI_FKey_cascade_upd
  - ri_set

## Notes and Other Information
- Handles snapshot management differently based on isolation level to ensure consistency
- Switches to table owner's security context to perform permission checks as the appropriate user
- Returns true if the query found matching rows, false otherwise
- May report constraint violations through ri_ReportViolation when appropriate
- Uses pre-compiled SPI plans for performance optimization
- Supports various referential integrity actions through different query types