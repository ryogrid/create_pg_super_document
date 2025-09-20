# heap_tableam_handler

## Location
[src/backend/access/heap/heapam_handler.c:2659-2662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L2659-L2662)

## Overview
heap_tableam_handler is a PostgreSQL function that serves as the access method handler for heap tables, returning the heap access method's TableAmRoutine structure.

## Definition
```c
Datum
heap_tableam_handler(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the official PostgreSQL access method handler function for heap tables. It is registered in the system catalogs (pg_proc.dat and pg_am.dat) and provides the entry point for the heap access method within PostgreSQL's pluggable table access method architecture.

The function follows PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS) and returns a Datum containing a pointer to the heapam_methods structure. This allows PostgreSQL's access method infrastructure to dynamically load and utilize the heap access method's complete function table.

Unlike GetHeapamTableAmRoutine() which is an internal C function, heap_tableam_handler() is exposed as a SQL-callable function that can be invoked by PostgreSQL's catalog system to obtain the heap access method interface. It is specifically referenced in pg_am.dat as the amhandler for the 'heap' access method type.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_POINTER (PostgreSQL macro)
  - heapam_methods (static TableAmRoutine structure)
- Called from (representative examples):
  - PostgreSQL's access method catalog system
  - Dynamic access method loading infrastructure

## Notes and Other Information
- This function is registered in the system catalog as the handler for the heap access method (pg_am.dat: amhandler => 'heap_tableam_handler')
- It is also declared as a PostgreSQL function in pg_proc.dat with volatile behavior
- The function serves as a bridge between PostgreSQL's SQL-level access method system and the internal C implementation
- Part of PostgreSQL's extensible table access method framework, allowing heap tables to be treated as a pluggable access method
- The function signature follows PostgreSQL's Version-1 function calling convention
- Critical for PostgreSQL's initialization and dynamic loading of the heap access method