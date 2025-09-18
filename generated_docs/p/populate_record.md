# populate_record

## Location
src/backend/utils/adt/jsonfuncs.c: 3518 - 3633

## Overview
A static function that converts JSON/JSONB values into PostgreSQL record (composite type) tuples by populating field values according to the provided tuple descriptor.

## Definition


## Detailed Description
This function takes a JSON object and populates a PostgreSQL record tuple based on the structure defined by the tuple descriptor. It handles field mapping, type conversion, and maintains metadata caching for performance optimization. The function supports default values and proper handling of dropped columns and domain types.

Key behaviors:
- Returns the default value immediately if the JSON object is empty and a default is provided
- Allocates or reuses metadata cache for column information
- Invalidates cache when record type changes
- Processes each column by matching JSON field names to column names
- Handles dropped columns by setting them to NULL
- Ensures domain type validation even for missing fields

## Parameters / Member Variables
- : Tuple descriptor defining the target record structure
- : Pointer to cached metadata for record I/O operations (allocated/reused)
- : Optional default tuple header to use for missing values
- : Memory context for allocations
- : JSON object containing the field values to populate
- : Error context for reporting conversion errors

## Dependencies
- Functions called/Symbols referenced:
  - JsObjectIsEmpty
  - [allocate_record_info](../a/allocate_record_info.md)
  - MemSet
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [JsObjectGetField](../J/JsObjectGetField.md)
  - [populate_record_field](populate_record_field.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - HeapTupleHeaderGetDatumLength
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
- Called from (representative examples):
  - [populate_composite](populate_composite.md)
  - [populate_recordset_record](populate_recordset_record.md)

## Notes and Other Information
- This is a static function used internally by JSON processing functions
- Implements efficient metadata caching to avoid repeated type lookups
- Properly handles PostgreSQL's tuple structure including dropped columns
- Ensures domain type constraints are validated even for missing JSON fields
- Memory management is handled through the provided memory context
- Part of PostgreSQL's JSON/JSONB to record conversion infrastructure