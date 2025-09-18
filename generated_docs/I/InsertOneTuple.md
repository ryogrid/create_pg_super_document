# InsertOneTuple

## Location
src/backend/bootstrap/bootstrap.c: 598 - 625

## Overview
InsertOneTuple creates and inserts a single tuple (row) into the currently open bootstrap relation using the accumulated attribute values and null markers.

## Definition
```c
void InsertOneTuple(void)
```

## Detailed Description
InsertOneTuple is a bootstrap function that constructs and inserts a complete tuple into the currently open relation during PostgreSQL system initialization. The function operates on global state maintained by the bootstrap process, including the `numattr` count, `attrtypes` array, `values` array, and `Nulls` array.

The insertion process involves several steps:
1. Creates a tuple descriptor from the current attribute definitions
2. Forms a heap tuple from the accumulated values and null indicators
3. Inserts the tuple into the current bootstrap relation using simple_heap_insert
4. Cleans up allocated memory and resets null markers for the next tuple

This function is typically called after a complete set of column values have been specified through InsertOneValue and InsertOneNull calls, representing one complete row of data for system catalog initialization.

## Parameters / Member Variables
This function takes no parameters and operates on global bootstrap state:
- Uses global `numattr` for the number of attributes
- Uses global `attrtypes` array for attribute type information  
- Uses global `values` array for column values
- Uses global `Nulls` array for null indicators
- Uses global `boot_reldesc` for the target relation

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTupleDesc](../C/CreateTupleDesc.md) (creates tuple descriptor from attributes)
  - [heap_form_tuple](../h/heap_form_tuple.md) (constructs heap tuple from values)
  - [pfree](../p/pfree.md) (frees tuple descriptor memory)
  - [simple_heap_insert](../s/simple_heap_insert.md) (inserts tuple into relation)
  - [heap_freetuple](../h/heap_freetuple.md) (frees heap tuple memory)
  - DEBUG4 (debug logging level)
- Called from (representative examples):
  - Bootstrap parser after accumulating complete row data

## Notes and Other Information
- This function is part of the bootstrap process and only used during PostgreSQL system initialization
- The function automatically resets all null markers to false after insertion, preparing for the next tuple
- Memory management is careful to free the tuple descriptor but preserve the attrtypes array for reuse
- Uses simple_heap_insert rather than the full heap_insert to avoid overhead during bootstrap
- Debug logging reports the number of columns being inserted and confirms successful insertion
- Operates on the assumption that a relation is currently open (boot_reldesc is valid)
- The function does not handle OID assignment explicitly - this is managed by the heap insertion routines