# pg_any_to_server

## Location
[src/backend/utils/mb/mbutils.c:676-737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L676-L737)

## Overview
A comprehensive character encoding conversion function that converts text from any specified encoding to the server's database encoding, with built-in validation and special handling for various encoding scenarios.

## Definition
```c
char *pg_any_to_server(const char *s, int len, int encoding)
```

## Detailed Description
The `pg_any_to_server` function is the core encoding conversion routine in PostgreSQL that handles conversion from any supported character encoding to the server's database encoding. Unlike other conversion functions, it always performs validation even when no conversion is needed, making it suitable for processing external data that cannot be assumed to be valid.

The function implements several optimization paths and special cases:
- Fast path for empty strings
- No-conversion path with validation when source matches database encoding
- Special handling for SQL_ASCII database encoding
- Cached conversion for client encoding
- General conversion path using the full conversion system

## Parameters / Member Variables
- `s`: Pointer to the input string to convert
- `len`: Length of the input string in bytes  
- `encoding`: Source character encoding ID

## Dependencies
- Functions called/Symbols referenced:
  - unconstify (removes const qualifier safely)
  - [pg_verify_mbstr](pg_verify_mbstr.md) (validates multibyte string)
  - PG_VALID_BE_ENCODING (validates backend encoding)
  - IS_HIGHBIT_SET (checks for high-bit characters)
  - [perform_default_encoding_conversion](perform_default_encoding_conversion.md) (cached client conversion)
  - [pg_do_encoding_conversion](pg_do_encoding_conversion.md) (general conversion routine)
- Called from (representative examples):
  - [read_extension_script_file](../r/read_extension_script_file.md) (extension script processing)
  - [X509_NAME_to_cstring](../X/X509_NAME_to_cstring.md) (SSL certificate processing)
  - [dsnowball_lexize](../d/dsnowball_lexize.md) (text search dictionary)
  - [pg_client_to_server](pg_client_to_server.md) (client encoding conversion wrapper)
  - [PLyUnicode_Bytes](../P/PLyUnicode_Bytes.md) (Python language interface)

## Notes and Other Information
- Always validates input data, even when no encoding conversion is required
- Returns the original string pointer when no conversion is needed (after validation)
- Has special logic for SQL_ASCII database encoding that rejects non-ASCII data for unsafe encodings
- Uses cached conversion functions when converting from client encoding for performance
- The general conversion path requires being inside a database transaction
- Critical function for maintaining data integrity when processing external input
- Located in src/backend/utils/mb/mbutils.c:676-737

## Simplified Source

```c
// Simplified version of pg_any_to_server
char *pg_any_to_server(const char *s, int len, int encoding) {
    // Empty strings are always valid
    if (len <= 0) {
        return unconstify(char *, s);
    }

    // No conversion needed - just validate the data
    if (encoding == DatabaseEncoding->encoding || encoding == PG_SQL_ASCII) {
        (void) pg_verify_mbstr(DatabaseEncoding->encoding, s, len, false);
        return unconstify(char *, s);
    }

    // Special case: database is SQL_ASCII
    if (DatabaseEncoding->encoding == PG_SQL_ASCII) {
        if (PG_VALID_BE_ENCODING(encoding)) {
            // Validate under the source encoding
            (void) pg_verify_mbstr(encoding, s, len, false);
        } else {
            // For unsafe encodings, reject any non-ASCII characters
            for (int i = 0; i < len; i++) {
                if (s[i] == '\0' || IS_HIGHBIT_SET(s[i])) {
                    ereport(ERROR,
                            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                             errmsg("invalid byte value for encoding \"%s\": 0x%02x",
                                    pg_enc2name_tbl[PG_SQL_ASCII].name,
                                    (unsigned char) s[i])));
                }
            }
        }
        return unconstify(char *, s);
    }

    // Fast path for client encoding conversion
    if (encoding == ClientEncoding->encoding) {
        return perform_default_encoding_conversion(s, len, true);
    }

    // General conversion case
    return (char *) pg_do_encoding_conversion((unsigned char *) unconstify(char *, s),
                                            len, encoding, DatabaseEncoding->encoding);
}
```

Key simplifications made:
- Organized the function into clear decision blocks
- Added comments explaining each conversion path
- Simplified the ASCII validation loop for readability
- Preserved all essential validation and conversion logic
- Maintained the optimization paths while improving clarity