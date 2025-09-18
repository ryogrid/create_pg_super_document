# expanded_record_set_fields

## Location
src/backend/utils/adt/expandedrecord.c: 1249 - 1378

## Overview
Sets all fields of an expanded record in one operation, ensuring proper memory management and type consistency for PostgreSQL record data structures.

## Definition


## Detailed Description
This function performs bulk assignment of field values to an expanded record, providing an efficient way to initialize or completely replace all fields at once. Unlike individual field assignments via , this function does not guarantee atomicity or corruption-free state in case of errors, making it primarily suitable for initializing new expanded records.

The function handles proper memory management by copying non-by-value fields into the record's memory context, optionally detoasting external TOAST values based on the  parameter. It maintains the expanded record's internal flags to track data validity and external dependencies.

## Parameters / Member Variables
- : Pointer to the ExpandedRecordHeader structure to modify
- : Array of Datum values to assign to record fields
- : Array of boolean flags indicating which fields are NULL
- : Boolean flag controlling whether to forcibly detoast external TOAST values

## Dependencies
- Functions called/Symbols referenced:
  - deconstruct_expanded_record
  - VARATT_IS_EXTERNAL
  - detoast_external_attr
  - datumCopy
  - get_short_term_cxt
  - domain_check
  - ExpandedRecordGetRODatum
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- Function assumes caller has verified that provided datums match the record's rowtype
- Does not guarantee atomicity - errors may leave record in corrupted state
- Primarily intended for initializing new expanded records rather than updating existing ones
- Automatically handles domain constraint checking if the record represents a domain type
- Sets ER_FLAG_DVALUES_ALLOCED when allocating memory for non-by-value fields
- Invalidates flattened representation (ER_FLAG_FVALUE_VALID) since fields have changed