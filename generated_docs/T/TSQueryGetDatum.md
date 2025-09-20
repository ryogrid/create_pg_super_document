# TSQueryGetDatum

## Location
[src/include/tsearch/ts_type.h:263-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_type.h#L263-L267)

## Overview
TSQueryGetDatum is a static inline function that converts a TSQueryData pointer to a PostgreSQL Datum representation, enabling TSQuery data structures to be passed through PostgreSQL's internal function calling interface.

## Definition

```c
static inline Datum
TSQueryGetDatum(const TSQueryData *X)
```
## Detailed Description
TSQueryGetDatum is a type conversion utility function that wraps a TSQueryData pointer into PostgreSQL's universal Datum type. This function is essential for PostgreSQL's internal architecture where all function arguments and return values must be represented as Datum types. The function simply delegates to PointerGetDatum(), which performs the actual pointer-to-Datum conversion by casting the pointer to a Datum type.

This function is part of PostgreSQL's text search functionality infrastructure, specifically designed to handle TSQuery (text search query) data structures within the PostgreSQL function calling conventions. It enables TSQuery objects to be seamlessly integrated with PostgreSQL's type system and function interface.

## Parameters / Member Variables
- : A const pointer to TSQueryData structure representing a text search query. This parameter cannot be NULL as it's directly passed to PointerGetDatum.

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md) (from src/include/postgres.h)
  - [TSQueryData](TSQueryData.md) (struct type from src/include/tsearch/ts_type.h)
- Called from (representative examples):
  - PG_RETURN_TSQUERY (macro in src/include/tsearch/ts_type.h:270)
  - [ts_match_tt](../t/ts_match_tt.md) (in src/backend/utils/adt/tsvector_op.c:2257)
  - ts_match_tq (in src/backend/utils/adt/tsvector_op.c:2277)

## Notes and Other Information
- This is a static inline function defined in a header file, meaning it's expanded at compile time for performance
- The function is part of a pair with DatumGetTSQuery(), which performs the reverse conversion
- It's commonly used through the PG_RETURN_TSQUERY macro for returning TSQuery values from PostgreSQL functions
- The TSQueryData structure contains a varlena header, size information, and flexible array member for query data
- This function is critical for PostgreSQL's text search functionality, allowing TSQuery objects to be properly handled by the database's type system