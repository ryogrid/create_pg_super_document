# jsonb_populate_record

## Location
src/backend/utils/adt/jsonfuncs.c: 2462 - 2474

## Overview
This SQL function populates a PostgreSQL record (row type) with field values from a JSONB object, mapping JSON key-value pairs to record columns by name.

## Definition


## Detailed Description
The  function implements the SQL function  which takes a record template and a JSONB object as input, returning a new record with fields populated from the JSONB data. This function serves as a bridge between JSONB data and PostgreSQL's structured record types.

The function works by decomposing the JSONB object and looking up each field in the target record type by name. For each matching field name, it extracts the corresponding value from the JSONB object and converts it to the appropriate PostgreSQL data type for that record field. Fields not present in the JSONB object retain their original values or remain NULL.

The implementation is adapted from hstore's  functionality and leverages the optimized access patterns available for JSONB data structures, where values can be fetched directly from the object without full parsing.

This function is particularly useful for converting JSON data into typed PostgreSQL records, enabling strong typing and validation while maintaining the flexibility of JSON input.

## Parameters / Member Variables
- Function uses  macro to access SQL function arguments:
  - : Base record to be populated (record type)
  - : JSONB object containing field values

## Dependencies
- Functions called/Symbols referenced:
  - [populate_record_worker](../p/populate_record_worker.md) (core implementation function)

- Called from (representative examples):
  - Direct SQL function calls
  - No internal PostgreSQL references found

## Notes and Other Information
- Part of PostgreSQL's JSONB to record conversion infrastructure
- Code adapted from hstore's populate_record implementation
- Uses direct value fetching from JSONB objects for efficiency
- Supports type coercion from JSON values to record field types
- Handles missing fields gracefully by preserving original values
- Essential for converting semi-structured JSONB data to structured records
- Callable from SQL as 