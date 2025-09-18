# GetIndexAmRoutine

## Location
src/backend/access/index/amapi.c: 33 - 55

## Overview
GetIndexAmRoutine calls the specified access method handler routine to get its IndexAmRoutine struct, which contains the function pointers and metadata for an index access method.

## Definition


## Detailed Description
This function serves as a central interface for obtaining the IndexAmRoutine structure from an access method handler function. It takes an OID of an access method handler function and calls it to retrieve the IndexAmRoutine struct that defines the operations supported by that index access method.

The function performs validation to ensure the handler returns a valid IndexAmRoutine struct. If the amhandler function is built-in, this operation does not involve any catalog access, making it safe to use during bootstrap when setting up indexes for system catalogs. The relcache.c module relies on this bootstrap-safe behavior.

The returned IndexAmRoutine struct is palloc'd in the caller's context and contains function pointers for all the operations that the index access method supports.

## Parameters / Member Variables
- : OID of the access method handler function that returns the IndexAmRoutine struct

## Dependencies
- Functions called/Symbols referenced:
  - OidFunctionCall0
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - IsA
  - elog
  - [IndexAmRoutine](../I/IndexAmRoutine.md) (struct type)
- Called from (representative examples):
  - [GetIndexAmRoutineByAmId](GetIndexAmRoutineByAmId.md)
  - [CheckIndexCompatible](../C/CheckIndexCompatible.md)
  - [DefineIndex](../D/DefineIndex.md)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)
  - [InitIndexAmRoutine](../I/InitIndexAmRoutine.md)

## Notes and Other Information
- Bootstrap-safe: Can be used during system catalog initialization since built-in handlers don't require catalog access
- Error handling: Validates that the handler function returns a proper IndexAmRoutine struct
- Memory management: The returned struct is allocated in the caller's memory context
- Critical for index access method abstraction layer in PostgreSQL