# tts_virtual_init

## Location
[src/backend/executor/execTuples.c:98-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L98-L102)

## Overview
Initializes a virtual tuple table slot, serving as the init callback for the TTSOpsVirtual operations structure.

## Definition


## Detailed Description
The  function is the initialization callback for virtual tuple table slots in PostgreSQL. It is part of the  operations structure that defines the behavior for virtual tuple slots. A virtual tuple table slot is a lightweight implementation that stores tuple data as arrays of Datum values and null indicators, without requiring physical storage of heap tuples or minimal tuples.

This function has an empty implementation because virtual tuple table slots require no special initialization beyond what is provided by the base  structure. The slot is already properly initialized when created, and virtual slots don't need additional setup for buffers, heap tuples, or other storage-specific resources.

## Parameters / Member Variables
- : A pointer to the TupleTableSlot being initialized. This will be a VirtualTupleTableSlot structure that extends the base TupleTableSlot.

## Dependencies
- Functions called/Symbols referenced: None (empty function body)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (via TTSOpsVirtual.init callback)
  - Various executor nodes that initialize virtual tuple table slots

## Notes and Other Information
- Virtual tuple table slots are one of several tuple slot implementations in PostgreSQL
- They are used when you need a lightweight way to store computed tuples without the overhead of heap tuple storage
- The empty implementation indicates that virtual slots are simple enough to not require custom initialization
- Part of PostgreSQL's tuple table slot abstraction that allows different storage implementations behind a common interface
- Virtual slots are commonly used for result tuples in various executor nodes like ProjectSet, NestLoop, HashJoin, etc.