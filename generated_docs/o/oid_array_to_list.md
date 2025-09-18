# oid_array_to_list

## Location
src/backend/catalog/pg_proc.c: 1184 - 1196

## Overview
Converts a PostgreSQL array of OIDs (Object Identifiers) into a PostgreSQL List data structure containing the individual OID values.

## Definition
List *oid_array_to_list(Datum datum)

## Detailed Description
This utility function takes a Datum representing an array of OIDs and converts it into a PostgreSQL List structure. It uses the built-in array deconstruction functions to extract the individual OID values from the array, then iterates through them to build a new List by appending each OID using lappend_oid().

The function is commonly used in PostgreSQL's procedural language implementations and other parts of the system where OID arrays need to be processed as lists for easier manipulation or iteration.

## Parameters / Member Variables
- `datum`: A Datum value containing an array of OIDs to be converted

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)  
  - lappend_oid
- Called from (representative examples):
  - [compile_plperl_function](../c/compile_plperl_function.md)
  - [PLy_procedure_create](../P/PLy_procedure_create.md)

## Notes and Other Information
- Returns a List containing the OID values from the input array
- Uses NIL as the initial empty list value
- Leverages PostgreSQL's built-in array handling with OIDOID type specification
- The function assumes the input datum represents a valid OID array
- Commonly used in procedural language contexts (PL/Perl, PL/Python) for processing function parameter type arrays
- The resulting List uses PostgreSQL's memory management and can be processed with standard List manipulation functions