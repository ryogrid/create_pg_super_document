# ExecSetOp

## Location
src/backend/executor/nodeSetOp.c: 190 - 226

## Overview
ExecSetOp is the main executor function for set operations (UNION, INTERSECT, EXCEPT), responsible for retrieving and returning the next tuple from a SetOp node according to the configured strategy.

## Definition


## Detailed Description
ExecSetOp implements the core execution logic for PostgreSQL's set operations. It handles two distinct strategies:

1. **Direct Strategy**: For sorted inputs, it directly processes tuples by comparing adjacent groups
2. **Hashed Strategy**: For unsorted inputs, it uses a hash table to group and count tuples

The function maintains state to handle cases where a tuple needs to be returned multiple times (for UNION ALL operations with duplicate counts). It tracks completion status through the  flag and manages output counting via .

The function follows PostgreSQL's executor pattern of returning one tuple per call, maintaining internal state between calls to track progress through the result set.

## Parameters / Member Variables
- : Pointer to the PlanState structure containing the SetOp node state and execution context

## Dependencies
- Functions called/Symbols referenced:
  - castNode (for type casting to SetOpState)
  - CHECK_FOR_INTERRUPTS (interrupt handling macro)
  - setop_fill_hash_table (populates hash table for hashed strategy)
  - setop_retrieve_hash_table (retrieves next tuple from hash table)
  - setop_retrieve_direct (retrieves next tuple using direct comparison)
- Called from (representative examples):
  - ExecInitSetOp (sets this as the execution function)

## Notes and Other Information
- Returns NULL when no more tuples are available (end of result set)
- Handles duplicate output counting for operations that require multiple returns of the same tuple
- Strategy selection (SETOP_HASHED vs direct) is determined at plan time based on input characteristics
- Part of PostgreSQL's executor framework for set operations (UNION, INTERSECT, EXCEPT)