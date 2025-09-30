# is_encoding_supported_by_icu

## Location
[src/common/encnames.c:461-471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/encnames.c#L461-L471)

## Overview
Checks whether a given character encoding is supported by the ICU (International Components for Unicode) library in PostgreSQL.

## Definition
bool is_encoding_supported_by_icu(int encoding)

## Detailed Description
This function determines if a specified character encoding can be used with ICU-based features in PostgreSQL, such as ICU collations and locale operations. It performs a two-step validation: first checking if the encoding is a valid backend encoding using the PG_VALID_BE_ENCODING macro, then verifying that there is a corresponding ICU encoding name available in the pg_enc2icu_tbl mapping table.

## Parameters / Member Variables
- encoding: Integer identifier representing a PostgreSQL character encoding (from the pg_enc enum)

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_BE_ENCODING (macro for validating backend encodings)
  - pg_enc2icu_tbl (global mapping table from PostgreSQL encodings to ICU encoding names)
- Called from (representative examples):
  - [lookup_collation](../l/lookup_collation.md) (src/backend/catalog/namespace.c:2351)
  - [DefineCollation](../D/DefineCollation.md) (src/backend/commands/collationcmds.c:337)  
  - [createdb](../c/createdb.md) (src/backend/commands/dbcommands.c:1106)
  - [check_icu_locale_encoding](../c/check_icu_locale_encoding.md) (src/bin/initdb/initdb.c:2284)

## Notes and Other Information
- Returns false for invalid backend encodings or encodings without ICU support
- Essential for ICU-related database operations like creating ICU collations or databases with ICU locales
- Located in src/common/encnames.c:461-471

## Simplified Source

```c
bool
is_encoding_supported_by_icu(int encoding)
{
    // Check if encoding is valid for backend use
    if (!PG_VALID_BE_ENCODING(encoding))
        return false;

    // Check if ICU mapping exists for this encoding
    return (pg_enc2icu_tbl[encoding] != NULL);
}
```