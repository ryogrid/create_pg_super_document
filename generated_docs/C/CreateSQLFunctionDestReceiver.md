# CreateSQLFunctionDestReceiver

## Location
src/backend/executor/functions.c: 2069 - 2087

## Overview
Creates and initializes a DestReceiver object specifically designed to handle result tuples from SQL function execution.

## Definition
```c
DestReceiver *CreateSQLFunctionDestReceiver(void)
```

## Detailed Description
CreateSQLFunctionDestReceiver is a factory function that creates a specialized DestReceiver object for SQL function execution contexts. The DestReceiver framework in PostgreSQL provides a standardized interface for handling query result tuples, and this particular implementation is tailored for SQL functions that need to process and potentially store result sets.

The function allocates and initializes a DR_sqlfunction structure, which is a specialized subtype of DestReceiver. It sets up the callback function pointers that define how the receiver handles different phases of result processing:
- Tuple reception during execution
- Startup operations before result processing begins  
- Shutdown operations after result processing completes
- Resource cleanup when the receiver is no longer needed

The created receiver is marked with DestSQLFunction destination type, identifying it within PostgreSQL's result destination framework.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - palloc0 (allocates zero-initialized memory)
  - sqlfunction_receive (callback for receiving individual tuples)
  - sqlfunction_startup (callback for initialization)
  - sqlfunction_shutdown (callback for cleanup)
  - sqlfunction_destroy (callback for resource deallocation)
  - DestSQLFunction (destination type constant)
- Called from (representative examples):
  - CreateDestReceiver (general destination receiver factory)

## Notes and Other Information
- Part of PostgreSQL's DestReceiver framework for handling query results
- The returned receiver requires additional configuration by postquel_start before use
- Essential component in SQL function execution pipeline for result processing
- Uses palloc0 to ensure all private fields start as NULL/zero
- The receiver's private fields are set later by postquel_start during actual execution setup  
- Integrates with PostgreSQL's memory context system for proper resource management
- Used specifically within the SQL function execution infrastructure to collect and process function result sets