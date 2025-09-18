# ExecInitBitmapOr

## Location
[src/backend/executor/nodeBitmapOr.c:56-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeBitmapOr.c#L56-L110)

## Overview
ExecInitBitmapOr initializes a BitmapOr executor node, setting up the state structure and initializing all child subplans for bitmap OR operations.

## Definition


## Detailed Description
ExecInitBitmapOr is responsible for initializing a BitmapOr executor node during query plan initialization. The function creates a BitmapOrState structure, allocates an array to hold pointers to child plan states, and recursively initializes each child subplan through ExecInitNode. The function sets up the execution context but notably does not create expression contexts or tuple slots since BitmapOr nodes operate on bitmaps rather than tuples and do not evaluate expressions.

The function validates that certain execution flags (EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK) are not set, as these are not supported by BitmapOr nodes. It assigns the ExecBitmapOr function as the execution procedure, though this function is designed to error if called directly.

## Parameters / Member Variables
- : Pointer to the BitmapOr plan node containing the configuration and child plans
- : Execution state containing global query execution context
- : Execution flags controlling execution behavior (some flags are explicitly not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates BitmapOrState structure)
  - list_length (determines number of child plans)
  - [palloc0](../p/palloc0.md) (allocates memory for child plan state array)
  - [ExecBitmapOr](ExecBitmapOr.md) (assigned as execution function)
  - [ExecInitNode](ExecInitNode.md) (recursively initializes child plans)
  - Assert (validates unsupported flags)

- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (part of the general node initialization dispatch)

## Notes and Other Information
- BitmapOr nodes do not create expression contexts since they never call ExecQual or ExecProject
- BitmapOr nodes do not need tuple slots as they work with bitmaps, not tuples
- The function performs validation to ensure backward scanning and mark/restore are not requested
- Child plans are initialized with the same execution flags as the parent
- Memory allocation uses palloc0 to ensure the array is zero-initialized