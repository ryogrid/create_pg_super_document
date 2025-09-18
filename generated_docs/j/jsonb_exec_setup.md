# jsonb_exec_setup

## Location
src/backend/utils/adt/jsonbsubs.c: 353 - 401

## Overview
Sets up execution state for a JSONB subscript operation, preparing workspace and method pointers for accessing JSONB data through subscripting syntax.

## Definition


## Detailed Description
This function initializes the execution state for JSONB subscript operations (e.g., ). Unlike array subscripting which has nesting limits, JSONB subscripting has no inherent nesting limitations since the JSONB type itself doesn't impose such restrictions.

The function allocates a type-specific workspace () that includes space for per-subscript data, collects subscript data types needed during execution, and sets up method pointers for the actual subscript operations. The workspace is carefully laid out in memory with proper alignment considerations.

## Parameters / Member Variables
- : Pointer to SubscriptingRef structure containing the subscript reference information including upper index expressions
- : Pointer to SubscriptingRefState structure where the allocated workspace will be stored
- : Pointer to SubscriptExecSteps structure that will be populated with function pointers for subscript operations

## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
  -  (memory alignment macro)
  -  (list cell access)
  -  (list iteration helper)
  -  (expression type determination)
  - 
  - 
  - 
  - 
- Data structures referenced:
  - 
  -   
  - 
  - 
- Called from:
  - 

## Notes and Other Information
- The function is static (internal to jsonbsubs.c file)
- Memory allocation includes careful alignment calculations to ensure proper pointer alignment
- The workspace expectArray field is set to false, distinguishing JSONB subscripting from array subscripting
- The function assumes  for proper memory alignment
- Located in 