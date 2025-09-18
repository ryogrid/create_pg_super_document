# DatumGetExpandedRecord

## Location
[src/backend/utils/adt/expandedrecord.c:927-951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L927-L951)

## Overview
Retrieves a writable expanded record from an input Datum, returning either the existing expanded record if it's already writable or creating a new expanded record if necessary.

## Definition


## Detailed Description
This function provides a safe way to obtain a writable expanded record from a Datum input. It first checks if the input Datum is already a writable expanded record using the VARATT_IS_EXTERNAL_EXPANDED_RW macro. If it is, the function simply returns the existing expanded record header after validating its magic number. If the input is not already a writable expanded record (e.g., it's a regular composite value or a read-only expanded record), the function calls make_expanded_record_from_datum to create a new expanded record in the current memory context.

The function includes an important safety caveat: when returning an existing writable expanded record, callers must ensure their modifications are "safe" and won't leave the record in a corrupt state, since they're working directly with the original data structure.

## Parameters / Member Variables
- : The input Datum that should contain a composite value or expanded record

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_EXPANDED_RW
  - [DatumGetPointer](DatumGetPointer.md)
  - DatumGetEOHP
  - [make_expanded_record_from_datum](../m/make_expanded_record_from_datum.md)
  - CurrentMemoryContext
- Types referenced:
  - ExpandedRecordHeader
  - ER_MAGIC (magic number constant)
- Called from (representative examples):
  - PG_GETARG_EXPANDED_RECORD (macro wrapper)

## Notes and Other Information
- The function performs magic number validation (ER_MAGIC) to ensure the expanded record header is valid
- Callers must be careful when modifying returned expanded records to avoid corruption
- The function automatically handles memory context management when creating new expanded records
- This is a key function in PostgreSQL's expanded object infrastructure for composite types