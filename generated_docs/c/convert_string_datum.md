# convert_string_datum

## Location
[src/backend/utils/adt/selfuncs.c:4658-4738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L4658-L4738)

## Overview
Converts a string-type Datum into a palloc'd, null-terminated C string, handling multiple PostgreSQL string types and applying locale-specific transformations when necessary.

## Definition

```c
static char *
convert_string_datum(Datum value, Oid typid, Oid collid, bool *failure)
```
## Detailed Description
This function serves as a universal converter for PostgreSQL's various string-type datums into standard C strings. It handles the complexity of PostgreSQL's type system by supporting multiple string representations and applying locale-specific transformations when operating in non-C locales.

The function performs type-specific conversion based on the typid parameter:
- **CHAROID**: Single character types are converted to 2-byte null-terminated strings
- **BPCHAROID/VARCHAROID/TEXTOID**: Text-based types are converted using TextDatumGetCString
- **NAMEOID**: PostgreSQL name types are converted using the NameData structure

For non-C locales, the function applies  transformation to ensure correct locale-specific sorting and comparison behavior. This is crucial for accurate selectivity estimation in different linguistic contexts.

The implementation includes Windows-specific error handling for strxfrm() failures and uses a two-pass approach to determine the required buffer size for transformed strings.

## Parameters / Member Variables
- : The Datum containing the string value to convert
- : The OID of the PostgreSQL data type (CHAROID, BPCHAROID, VARCHAROID, TEXTOID, or NAMEOID)
- : The collation OID for locale-specific processing
- : Output pointer set to true if conversion fails due to unsupported type

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetChar](../D/DatumGetChar.md) (for character type conversion)
  - TextDatumGetCString (for text type conversion)
  - [NameData](../N/NameData.md) (structure for name type handling)
  - [lc_collate_is_c](../l/lc_collate_is_c.md) (locale checking)
  - PG_USED_FOR_ASSERTS_ONLY (assertion macro)
  - [palloc](../p/palloc.md), pstrdup, pfree (PostgreSQL memory management)
  - strxfrm (standard C locale transformation function)
- Called from (representative examples):
  - [convert_to_scalar](convert_to_scalar.md) (called 3 times for value, lobound, and hibound conversion)

## Notes and Other Information
- The function is static, indicating it's an internal utility within selfuncs.c
- Returns NULL and sets *failure to true for unsupported data types
- Locale transformation using strxfrm() is essential for accurate selectivity estimation in non-English databases
- Windows-specific handling addresses UTF-8 encoding issues with strxfrm() on that platform
- The two-pass strxfrm() approach (first to determine size, second to perform transformation) is standard practice
- Memory management follows PostgreSQL conventions using palloc/pfree
- Part of the selectivity estimation infrastructure used by PostgreSQL's query planner
- The locale transformation ensures that string comparisons used in selectivity calculations match the database's collation rules