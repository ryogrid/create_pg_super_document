# pg_encoding_to_char

## Location
[src/common/encnames.c:587-597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/encnames.c#L587-L597)

## Overview
Returns the canonical name of a PostgreSQL encoding given its encoding ID, or an empty string if the encoding ID is invalid.

## Definition

```c
const char *
pg_encoding_to_char(int encoding)
```
## Detailed Description
The `pg_encoding_to_char` function provides the reverse mapping of `pg_char_to_encoding`, converting an encoding ID back to its canonical string name. It performs a direct array lookup in the `pg_enc2name_tbl` table, which is indexed by encoding ID and contains the official encoding names.

The function first validates that the encoding ID is within the valid range using the `PG_VALID_ENCODING` macro. If valid, it accesses the corresponding entry in the encoding-to-name table and returns the canonical name string. If the encoding ID is invalid or out of range, the function returns an empty string rather than NULL.

An assertion check ensures that the table entry's encoding field matches the requested encoding ID, providing a debugging safeguard against table inconsistencies.

## Parameters / Member Variables
- `encoding`: The PostgreSQL encoding ID to convert to a string name

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_ENCODING (macro to validate encoding IDs)
  - Assert (debugging assertion macro)
- Data structures:
  - pg_enc2name_tbl (array of encoding ID to name mappings)
  - [pg_enc2name](pg_enc2name.md) (structure containing name, encoding, and optional codepage)
- Called from (representative examples):
  - [CollationCreate](../C/CollationCreate.md)
  - [ConversionCreate](../C/ConversionCreate.md)  
  - [createdb](../c/createdb.md)
  - [check_client_encoding](../c/check_client_encoding.md)
  - [pg_do_encoding_conversion](pg_do_encoding_conversion.md)
  - [PQsetClientEncoding](../P/PQsetClientEncoding.md)

## Notes and Other Information
- Returns an empty string ("") rather than NULL for invalid encoding IDs
- The returned string points to a constant string literal, no memory management needed
- The `pg_enc2name_tbl` is indexed directly by encoding ID for O(1) lookup performance
- On Windows builds, the structure also includes codepage information
- Widely used throughout PostgreSQL for displaying encoding names in error messages, logs, and user interfaces
- The canonical names returned are the "official" PostgreSQL encoding names, which may differ from aliases accepted by `pg_char_to_encoding`