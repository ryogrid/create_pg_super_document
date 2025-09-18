# make_tuple_indirect

## Location
[src/test/regress/regress.c:552-650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L552-L650)

## Overview
A PostgreSQL test function that converts regular tuple attributes into indirect TOAST pointers for testing TOAST (The Oversized-Attribute Storage Technique) functionality with indirect references.

## Definition


## Detailed Description
The  function is a specialized test utility that transforms a regular PostgreSQL tuple by converting its variable-length attributes into indirect TOAST pointers. This function is designed to test PostgreSQL's TOAST mechanism, specifically the indirect pointer functionality. It takes a HeapTupleHeader as input, decomposes the tuple into its constituent values, and then for each variable-length attribute that meets certain criteria (not dropped, not null, variable length, not plain storage), it creates an indirect pointer that references the original data. The function creates a new tuple structure where the original data is stored separately and accessed through indirect pointers. This enables testing of TOAST detoasting behavior and indirect pointer handling throughout the PostgreSQL system.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro providing access to function call context and arguments
- : Input HeapTupleHeader containing the tuple data to be processed
- : Temporary HeapTupleData structure for tuple manipulation
- : Array of Datum values extracted from the original tuple
- : Array of boolean flags indicating null values
- : OID of the tuple's row type
- : Type modifier for the tuple type
- : Tuple descriptor containing metadata about the tuple structure

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract HeapTupleHeader from function arguments
  - : Extracts type OID from tuple header
  - : Extracts type modifier from tuple header
  - : Looks up tuple descriptor for row type
  - : Gets length of tuple data
  - : Sets item pointer to invalid state
  - : Decomposes tuple into values and nulls arrays
  - : Detoasts externally stored attributes
  - : Creates new tuple from values and nulls
  - : Releases tuple descriptor reference
  - /: PostgreSQL memory allocation functions
  - : PostgreSQL memory deallocation function
- Called from (representative examples):
  - : Referenced in the same test regression file

## Notes and Other Information
- This is a test function located in the PostgreSQL regression test suite
- The function operates specifically on variable-length attributes (attlen == -1)
- Skips attributes that are dropped, null, fixed-length, or have plain storage
- Does not recursively create indirect pointers for already-indirect attributes
- Uses TopTransactionContext for memory allocation to ensure data persistence
- The function intentionally violates the general rule about composite Datums containing TOAST pointers for testing purposes
- Critical for testing TOAST functionality, particularly indirect pointer detoasting
- The returned tuple contains indirect pointers that must be handled carefully to avoid premature flattening