# DatumGetJsonbP

## Location
src/include/utils/jsonb.h: 374 - 379

## Overview
DatumGetJsonbP is a convenience macro function that converts a Datum value to a Jsonb pointer, handling TOAST decompression automatically.

## Definition


## Detailed Description
This inline function provides a convenient way to extract a Jsonb pointer from a Datum value. It internally uses the PG_DETOAST_DATUM macro to handle potentially TOASTed (The Oversized-Attribute Storage Technique) data, ensuring that if the Jsonb value was stored in compressed or out-of-line format, it gets properly decompressed and made accessible. The function is commonly used throughout PostgreSQL's JSONB handling code to safely convert Datum values received from the PostgreSQL type system into usable Jsonb structures.

## Parameters / Member Variables
- : The input Datum value that contains a JSONB value, potentially in TOASTed form

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM
  - Jsonb (type)
- Called from (representative examples):
  - ExecEvalJsonIsPredicate
  - datum_to_jsonb_internal
  - jsonb_subscript_fetch
  - jsonb_subscript_assign
  - JsonItemFromDatum
  - JsonPathExists
  - JsonPathQuery
  - JsonPathValue

## Notes and Other Information
- This is a static inline function defined in src/include/utils/jsonb.h, making it efficient for frequent use
- The function automatically handles TOAST decompression, which is crucial for large JSONB values that may be stored out-of-line
- Used extensively throughout the JSONB subsystem for type conversion from Datum to Jsonb pointer
- Part of the convenience macro family alongside DatumGetJsonbPCopy and JsonbPGetDatum
- The macro is used by PG_GETARG_JSONB_P for extracting function arguments