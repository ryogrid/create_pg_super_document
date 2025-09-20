# jsonb_subscript_handler

## Location
[src/backend/utils/adt/jsonbsubs.c:402-413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonbsubs.c#L402-L413)

## Overview
Main subscripting handler for JSONB data type that provides the interface for subscript operations on JSONB values by returning a pointer to subscript routine definitions.

## Definition
```c
Datum jsonb_subscript_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the entry point for JSONB subscripting operations in PostgreSQL. It defines and returns a static `SubscriptRoutines` structure that contains function pointers and behavior flags for handling JSONB subscript operations. The handler establishes the contract for how JSONB subscripting should behave, including transformation, execution setup, and safety characteristics.

The function is designed to be called by PostgreSQL's type system when subscript operations are performed on JSONB values. It provides a centralized configuration point for all JSONB subscripting behavior.

## Parameters / Member Variables
- Returns a `SubscriptRoutines` structure containing:
  - `transform`: Pointer to `jsonb_subscript_transform` function for parse-time transformations  
  - `exec_setup`: Pointer to `jsonb_exec_setup` function for execution state setup
  - `fetch_strict`: Set to `true` - fetch operations return NULL for NULL inputs
  - `fetch_leakproof`: Set to `true` - fetch operations return NULL for bad subscripts (no errors leaked)
  - `store_leakproof`: Set to `false` - assignment operations can throw errors

## Dependencies
- Functions called/Symbols referenced:
  - `PG_RETURN_POINTER` (PostgreSQL return macro)
  - [jsonb_subscript_transform](jsonb_subscript_transform.md) (transform function)
  - [jsonb_exec_setup](jsonb_exec_setup.md) (execution setup function)
- Data structures referenced:
  - `SubscriptRoutines`
- Called from:
  - PostgreSQL type system (no direct references found in codebase)

## Notes and Other Information
- The function uses PostgreSQL's standard function calling convention with `PG_FUNCTION_ARGS`
- The `SubscriptRoutines` structure is defined as static, meaning it's shared across all calls
- The leakproof settings indicate security characteristics: fetch operations are safe and don't leak information through errors, while store operations may reveal information through error messages
- This handler specifically configures JSONB to have strict fetch behavior (NULL for NULL) but non-leakproof store behavior
- Located in `src/backend/utils/adt/jsonbsubs.c:402-413`