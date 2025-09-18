# make_scalar_key

## Location
src/backend/utils/adt/jsonb_gin.c: 1364 - 1409

## Overview
This static function converts JsonbValue scalar types (null, boolean, numeric, string) into standardized GIN index keys with appropriate type flags for efficient indexing and querying.

## Definition
static Datum make_scalar_key(const JsonbValue *scalarVal, bool is_key)

## Detailed Description
The make_scalar_key function is responsible for converting JSONB scalar values into a uniform text-based representation suitable for storage in GIN indexes. It handles all four scalar JSONB types (null, boolean, numeric, string) by applying type-specific processing and encoding each value with an appropriate flag that identifies its original type.

For null values, it creates an empty text key with the JGINFLAG_NULL flag. Boolean values are converted to single-character representations ('t' for true, 'f' for false) with the JGINFLAG_BOOL flag. Numeric values undergo normalization to ensure mathematically equivalent numbers produce identical string representations, removing trailing zeros and standardizing format before storage with the JGINFLAG_NUM flag.

String values receive special handling based on the is_key parameter: when is_key is true (indicating the string represents a JSON object key or array element treated as a key), it uses the JGINFLAG_KEY flag; otherwise, it uses JGINFLAG_STR for regular string values. This distinction allows the GIN index to differentiate between object keys and string values during query processing.

All processing ultimately delegates to make_text_key() for the actual Datum construction, ensuring consistent key format and automatic handling of overlength values through hashing.

## Parameters / Member Variables
- : Pointer to JsonbValue structure containing the scalar value to be converted to an index key
- : Boolean flag indicating whether this string value should be treated as an object key (true) or regular string value (false)

## Dependencies
- Functions called/Symbols referenced:
  - make_text_key
  - numeric_normalize
  - strlen
  - pfree
  - elog
- Constants/Flags:
  - JGINFLAG_NULL
  - JGINFLAG_BOOL
  - JGINFLAG_NUM
  - JGINFLAG_KEY
  - JGINFLAG_STR
- Enum values:
  - jbvNull
  - jbvBool
  - jbvNumeric
  - jbvString
- Called from (representative examples):
  - gin_extract_jsonb
  - make_jsp_entry_node_scalar

## Notes and Other Information
The function includes assertions that null and boolean values cannot be object keys, reflecting JSON semantic constraints. For numeric values, the use of textual representation in the index is acknowledged as suboptimal for storage efficiency, but provides notational convenience for the GIN B-Tree union type storage and prioritizes string indexing performance. The normalization of numeric values is crucial for ensuring that mathematically equivalent numbers (like 1.0 and 1.00) produce identical index keys, enabling proper equality matching. The distinction between JGINFLAG_KEY and JGINFLAG_STR allows the index to support different query semantics for object keys versus string values, which is important for operators like the existence operator (?). The function serves as a critical bridge between JSONB's varied scalar types and the uniform text-based key format required by the GIN index infrastructure.