# TSQuerySignGetDatum

## Location
src/include/tsearch/ts_utils.h: 254 - 259

## Overview
A static inline function that converts a TSQuerySign value into a PostgreSQL Datum for use in the database system's internal representation.

## Definition


## Detailed Description
TSQuerySignGetDatum is a conversion utility function that transforms a TSQuerySign value (which is a typedef for uint64) into a Datum representation. This function is part of PostgreSQL's text search infrastructure and is used internally by the GiST indexing mechanism for tsquery operations. The function simply wraps the Int64GetDatum conversion, providing a type-safe interface specifically for TSQuerySign values.

This function is primarily used in the GiST (Generalized Search Tree) implementation for tsquery compression and node splitting operations, where TSQuerySign values need to be stored or manipulated as Datum objects.

## Parameters / Member Variables
- : The TSQuerySign value (uint64) to be converted into a Datum representation

## Dependencies
- Functions called/Symbols referenced:
  - Int64GetDatum (converts int64 to Datum)
  - TSQuerySign (typedef for uint64)
- Called from (representative examples):
  - gtsquery_compress (src/backend/utils/adt/tsquery_gist.c:39)
  - gtsquery_picksplit (src/backend/utils/adt/tsquery_gist.c:259, 260)
  - PG_RETURN_TSQUERYSIGN macro (src/include/tsearch/ts_utils.h:265)

## Notes and Other Information
- This is a static inline function defined in src/include/tsearch/ts_utils.h:254-259
- TSQuerySign is a typedef for uint64, representing a signature used for text search query optimization
- The function provides type safety by ensuring TSQuerySign values are properly converted to Datum format
- Primarily used in GiST indexing operations for efficient tsquery processing
- Part of PostgreSQL's full-text search functionality infrastructure