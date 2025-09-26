# ExecDropStmt

## Location
src/backend/tcop/utility.c: 1993 - 2025

## Overview
ExecDropStmt is a dispatch function that handles the execution of DROP statements for various database objects, routing the request to the appropriate removal function based on the object type.

## Definition


## Detailed Description
ExecDropStmt serves as a central dispatcher for DROP statement execution in PostgreSQL. It examines the removeType field of the DropStmt structure to determine what type of object is being dropped and calls the appropriate removal function. The function handles two main categories of objects:

1. **Relation-like objects** (tables, indexes, sequences, views, materialized views, foreign tables) - handled by RemoveRelations()
2. **Other database objects** - handled by RemoveObjects()

For concurrent index drops (DROP INDEX CONCURRENTLY), the function enforces transaction block restrictions by calling PreventInTransactionBlock to ensure the operation cannot run within a transaction block, as concurrent operations require special handling.

## Parameters / Member Variables
- : Pointer to a DropStmt structure containing details about the DROP operation, including the object type and names
- : Boolean flag indicating whether this statement is being executed at the top level (not within another statement)

## Dependencies
- Functions called/Symbols referenced:
  - PreventInTransactionBlock (for concurrent index drops)
  - RemoveRelations (for relation-like objects)
  - RemoveObjects (for other database objects)
  - Object type constants: OBJECT_INDEX, OBJECT_TABLE, OBJECT_SEQUENCE, OBJECT_VIEW, OBJECT_MATVIEW, OBJECT_FOREIGN_TABLE

- Called from:
  - standard_ProcessUtility
  - ProcessUtilitySlow

## Notes and Other Information
- This function is static and only accessible within the utility.c module
- Special handling for concurrent index drops prevents them from running within transaction blocks
- The function uses a switch statement to efficiently route different object types to their respective handlers
- Part of PostgreSQL's utility command processing infrastructure