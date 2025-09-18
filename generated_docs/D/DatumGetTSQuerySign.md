# DatumGetTSQuerySign

## Location
src/include/tsearch/ts_utils.h: 260 - 264

## Overview
A static inline function that converts a PostgreSQL Datum value back into a TSQuerySign for use in text search operations.

## Definition
static inline TSQuerySign DatumGetTSQuerySign(Datum X)

## Detailed Description
DatumGetTSQuerySign is a conversion utility function that transforms a Datum representation back into a TSQuerySign value (uint64). This function is the counterpart to TSQuerySignGetDatum and is essential for the GiST indexing mechanism in PostgreSQL's text search infrastructure. The function extracts the underlying int64 value from the Datum using DatumGetInt64 and casts it to TSQuerySign.

This function is primarily used in GiST index operations where TSQuerySign values stored as Datum objects need to be retrieved and used for query processing, consistency checking, penalty calculations, and entry retrieval operations.

## Parameters / Member Variables
- X: The Datum value containing a TSQuerySign to be extracted and converted back to TSQuerySign type

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInt64 (extracts int64 from Datum)
  - TSQuerySign (typedef for uint64)
- Called from (representative examples):
  - GETENTRY macro (src/backend/utils/adt/tsquery_gist.c:23)
  - gtsquery_consistent (src/backend/utils/adt/tsquery_gist.c:61)
  - gtsquery_penalty (src/backend/utils/adt/tsquery_gist.c:141, 142)
  - PG_GETARG_TSQUERYSIGN macro (src/include/tsearch/ts_utils.h:266)

## Notes and Other Information
- This is a static inline function defined in src/include/tsearch/ts_utils.h:260-264
- Works as the inverse operation of TSQuerySignGetDatum for bidirectional conversion
- TSQuerySign represents a signature used for optimizing text search query operations
- Essential for GiST index functionality including consistency checks, penalty calculations, and entry management
- Part of PostgreSQL's full-text search system that enables efficient indexing and querying of text data
- The cast to TSQuerySign ensures type safety when converting from the generic Datum representation