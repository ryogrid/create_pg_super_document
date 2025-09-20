# PLy_procedure_delete

## Location
[src/pl/plpython/plpy_procedure.c:403-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_procedure.c#L403-L414)

## Overview
PLy_procedure_delete is a cleanup function that deallocates memory and resources associated with a PLyProcedure object in PostgreSQL's PL/Python procedural language.

## Definition

```c
void
PLy_procedure_delete(PLyProcedure *proc)
```
## Detailed Description
This function performs comprehensive cleanup of a PLyProcedure structure by releasing all Python objects and memory contexts associated with it. It uses Python's reference counting mechanism (Py_XDECREF) to safely decrement reference counts for Python objects, which may trigger their garbage collection. The function also deletes the memory context that was allocated for the procedure, ensuring no memory leaks occur when a PL/Python procedure is no longer needed.

## Parameters / Member Variables
- : Pointer to the PLyProcedure structure to be cleaned up and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - Py_XDECREF (Python C API macro for safe reference decrementing)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (PostgreSQL memory management function)
  - [PLyProcedure](PLyProcedure.md) (structure type)
- Called from (representative examples):
  - [plpython3_inline_handler](../p/plpython3_inline_handler.md)
  - [PLy_procedure_get](PLy_procedure_get.md)
  - [PLy_procedure_create](PLy_procedure_create.md)

## Notes and Other Information
- Uses Py_XDECREF instead of Py_DECREF to safely handle NULL pointers
- The function assumes the procedure's memory context (proc->mcxt) is valid
- This is part of the resource management pattern in PL/Python extension
- Called during error handling and normal procedure lifecycle management