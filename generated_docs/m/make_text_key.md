# make_text_key

## Location
[src/backend/utils/adt/jsonb_gin.c:1326-1363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L1326-L1363)

## Overview
This static function constructs GIN index keys for JSONB text values by combining a flag byte with textual content, automatically hashing overlength strings to ensure key size limits.

## Definition
static Datum make_text_key(char flag, const char *str, int len)

## Detailed Description
The make_text_key function is a utility function used internally within the JSONB GIN indexing implementation to create standardized index keys for text-based JSONB values. It takes a flag byte (indicating the type and properties of the JSONB element), a string pointer, and length, then constructs a text Datum suitable for storage in a GIN index.

The function implements an important optimization for handling long text values: when the input string exceeds JGIN_MAXLENGTH, it computes a hash of the original string and stores the 8-character hexadecimal hash representation instead. This ensures that index keys remain within reasonable size bounds while still providing good selectivity. When hashing occurs, the function sets the JGINFLAG_HASHED bit in the flag to indicate this transformation.

The resulting Datum uses PostgreSQL's standard text format with a flag byte as the first character followed by the actual text content. The function builds a 4-byte-header varlena structure, though it expects this will be converted to short header format when stored in the index for space efficiency.

## Parameters / Member Variables
- : Character flag indicating JSONB element type and properties (will be modified with JGINFLAG_HASHED if hashing occurs)
- : Pointer to the string content to be stored in the key (need not be null-terminated)
- : Length of the string content in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [hash_any](../h/hash_any.md)
  - [DatumGetUInt32](../D/DatumGetUInt32.md)
  - snprintf
  - [palloc](../p/palloc.md)
  - SET_VARSIZE
  - VARDATA
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - memcpy
- Constants/Macros:
  - JGIN_MAXLENGTH
  - JGINFLAG_HASHED
  - VARHDRSZ
- Types used:
  - [text](../t/text.md)
  - Datum
- Called from (representative examples):
  - [jsonb_ops__add_path_item](../j/jsonb_ops__add_path_item.md)
  - [gin_extract_jsonb_query](../g/gin_extract_jsonb_query.md)
  - [make_scalar_key](make_scalar_key.md)

## Notes and Other Information
The function is critical for ensuring consistent key representation in JSONB GIN indexes while managing storage efficiency. The hashing mechanism prevents extremely long text values from consuming excessive index space, though it does introduce the possibility of hash collisions for very long strings. The flag byte encoding allows the GIN index to efficiently distinguish between different types of JSONB elements and their processing requirements. The function always builds 4-byte header varlena structures for simplicity, relying on PostgreSQL's automatic compression to short header format during index storage.