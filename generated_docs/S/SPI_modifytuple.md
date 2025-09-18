# SPI_modifytuple

## Location
[src/backend/executor/spi.c:1106-1174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1106-L1174)

## Overview
Creates a modified copy of a tuple by replacing specified attribute values, used for updating tuples in stored procedures and trigger functions.

## Definition


## Detailed Description
SPI_modifytuple creates a new HeapTuple that is a copy of the input tuple with specified attributes modified to new values. This function is commonly used in trigger functions and stored procedures where you need to modify some fields of a tuple while preserving others. The function decomposes the original tuple, replaces the specified attribute values, and then reconstructs a new tuple with the modified data.

The function preserves important tuple identification information (t_ctid, t_self, and t_tableOid) from the original tuple to the modified tuple. It performs comprehensive validation of input parameters and attribute numbers to ensure data integrity.

## Parameters / Member Variables
- : The relation (table) that defines the tuple structure and attribute information
- : The original HeapTuple to be modified
- : The number of attributes to be modified
- : Array of 1-based attribute numbers indicating which attributes to modify
- : Array of new Datum values to replace the existing attribute values
- : Optional array of null indicators ('n' means null, anything else means not null)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ttdummy](../t/ttdummy.md) (trigger function in regression tests)
  - Various custom trigger functions
  - Stored procedures that need to modify tuple data

## Notes and Other Information
- Sets SPI_result to SPI_ERROR_ARGUMENT if any required parameter is NULL or natts < 0
- Sets SPI_result to SPI_ERROR_UNCONNECTED if no SPI connection is active
- Sets SPI_result to SPI_ERROR_NOATTRIBUTE if any attribute number is invalid (≤ 0 or > number of attributes)
- Attribute numbers are 1-based, not 0-based
- The Nulls parameter can be NULL if no attributes should be set to null
- Returns NULL on any error condition
- The returned tuple is allocated in the upper executor's memory context
- Commonly used in BEFORE UPDATE triggers to modify the NEW tuple