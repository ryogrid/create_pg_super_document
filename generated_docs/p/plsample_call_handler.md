# plsample_call_handler

## Location
[src/test/modules/plsample/plsample.c:39-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/plsample/plsample.c#L39-L92)

## Overview
The main entry point for the plsample procedural language that routes function, procedure, and trigger calls to their appropriate handlers based on the call context.

## Definition


## Detailed Description
 serves as the central dispatcher for the plsample procedural language module, which is a test/example procedural language implementation in PostgreSQL. This function examines the calling context to determine whether it was invoked as a regular function, trigger function, or event trigger, then delegates execution to the appropriate specialized handler. The function implements proper exception handling using PostgreSQL's PG_TRY/PG_FINALLY/PG_END_TRY macros to ensure cleanup occurs even when errors are encountered.

The function uses PostgreSQL's function call information structure (fcinfo) to determine the execution context and routes calls accordingly:
- Regular function calls are handled by 
- Trigger function calls are handled by   
- Event trigger calls are recognized but not yet implemented (marked as TODO)

## Parameters / Member Variables
This function uses the standard PostgreSQL  macro which provides access to:
- : Function call information structure containing arguments, context, and metadata
- The context field determines whether this is a trigger, event trigger, or regular function call

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL exception handling macro)
  -  (macro to check if called as trigger)
  -  (macro to check if called as event trigger)
  -  (handles trigger function calls)
  -  (handles regular function calls)
  -  (converts pointer to Datum)
  -  (cleanup block macro)
  -  (end exception handling block)
- Called from:
  - This is a top-level entry point function, typically registered as a language handler in the PostgreSQL system catalogs

## Notes and Other Information
- Located in 
- This is part of PostgreSQL's test infrastructure, serving as an example implementation of a procedural language
- The function includes a framework for cleanup operations in the PG_FINALLY block, though the current implementation doesn't require any specific cleanup
- Event trigger functionality is recognized but not yet implemented (contains TODO comment)
- Follows PostgreSQL's standard pattern for language handlers that need to support multiple call contexts
- The return value is a Datum, which is PostgreSQL's generic data type for function return values