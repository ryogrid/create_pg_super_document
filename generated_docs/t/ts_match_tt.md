# ts_match_tt

## Location
[src/backend/utils/adt/tsvector_op.c:2244-2265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2244-L2265)

## Overview
A convenience function that performs text search matching between two plain text strings by automatically converting them to tsvector and tsquery formats.

## Definition

```c
Datum
ts_match_tt(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides a high-level interface for text search operations by accepting two plain text arguments and automatically performing the necessary conversions before matching. It converts the first argument to a tsvector using to_tsvector() and the second argument to a tsquery using plainto_tsquery(), then delegates to ts_match_vq for the actual matching logic.

This function simplifies text search for users who want to perform matching without manually creating tsvector and tsquery objects, making text search more accessible for basic use cases.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro which provides access to:
  - Argument 0: Text (document text to be converted to tsvector)
  - Argument 1: Text (query text to be converted to tsquery)

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall1
  - DirectFunctionCall2
  - [to_tsvector](to_tsvector.md)
  - [plainto_tsquery](../p/plainto_tsquery.md)
  - [ts_match_vq](ts_match_vq.md)
  - [DatumGetTSVector](../D/DatumGetTSVector.md)
  - [DatumGetTSQuery](../D/DatumGetTSQuery.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - [TSVectorGetDatum](../T/TSVectorGetDatum.md)
  - [TSQueryGetDatum](../T/TSQueryGetDatum.md)
  - [pfree](../p/pfree.md)
  - PG_GETARG_DATUM
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found (likely called via SQL operator system)

## Dependencies
- Functions called/Symbols referenced:
  - [to_tsvector](to_tsvector.md) (converts text to tsvector)
  - [plainto_tsquery](../p/plainto_tsquery.md) (converts text to tsquery)
  - [ts_match_vq](ts_match_vq.md) (performs actual matching)

## Notes and Other Information
- Automatically handles text-to-tsvector conversion using default text search configuration
- Uses plainto_tsquery for query conversion, which handles plain text queries without special syntax
- Manages memory properly by freeing intermediate tsvector and tsquery objects
- Provides a simplified interface for basic text search without requiring knowledge of tsvector/tsquery creation
- Part of PostgreSQL's text search operator overloading system for text @@ text operations