# fmtIdEnc

## Location
[src/fe_utils/string_utils.c:101-247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L101-L247)

## Overview
Formats and quotes PostgreSQL identifiers for safe use in SQL statements, with explicit encoding specification for proper multibyte character handling.

## Definition
```c
const char *fmtIdEnc(const char *rawid, int encoding)
```

## Detailed Description
This function takes a raw identifier string and returns a properly quoted and escaped version suitable for use in SQL statements. It performs comprehensive validation to determine whether quoting is necessary based on SQL identifier rules, reserved keywords, and character encoding considerations.

The function implements several layers of validation:

1. **Character validation**: Checks if the identifier contains only valid SQL identifier characters (a-z, 0-9, underscore)
2. **Keyword checking**: Uses ScanKeywordLookup() to identify SQL reserved keywords that require quoting
3. **Encoding-aware processing**: Handles multibyte characters properly according to the specified encoding
4. **Security validation**: Validates multibyte character sequences to prevent encoding-based security issues

When quoting is needed, the function wraps the identifier in double quotes and escapes any embedded double quotes by doubling them (per SQL standard). It also handles invalid multibyte sequences by replacing them with encoding-specific invalid sequences that will trigger server-side errors.

The function uses a shared buffer from getLocalPQExpBuffer(), so the returned string is only valid until the next call to any function using the same buffer.

## Parameters / Member Variables
- `rawid`: The input identifier string to be formatted
- `encoding`: PostgreSQL encoding constant (e.g., PG_UTF8, PG_LATIN1) for proper multibyte character handling

## Dependencies
- Functions called/Symbols referenced:
  - getLocalPQExpBuffer (for temporary buffer allocation)
  - [ScanKeywordLookup](../S/ScanKeywordLookup.md) (for keyword validation)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)/appendPQExpBufferChar (for buffer operations)
  - [pg_encoding_mblen](../p/pg_encoding_mblen.md) (for multibyte character length)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md) (for multibyte character validation)
  - [pg_encoding_set_invalid](../p/pg_encoding_set_invalid.md) (for invalid character replacement)
  - [enlargePQExpBuffer](../e/enlargePQExpBuffer.md) (for buffer expansion)
  - IS_HIGHBIT_SET (macro for multibyte detection)
- Called from (representative examples):
  - [fmtId](fmtId.md) (string_utils.c:250)
  - [fmtQualifiedIdEnc](fmtQualifiedIdEnc.md) (string_utils.c:271, 273)
  - [appendPsqlMetaConnect](../a/appendPsqlMetaConnect.md) (string_utils.c:795, 802)
  - [main](../m/main.md) functions in dropdb.c, dropuser.c
  - [gen_reindex_command](../g/gen_reindex_command.md) (reindexdb.c:528, 569)

## Notes and Other Information
- Returns pointer to shared buffer data - not thread-safe and not reentrant
- Follows PostgreSQL scan.l identifier production rules exactly
- Quotes all identifiers if quote_all_identifiers global flag is set
- Handles SQL99-compliant double-quote escaping within quoted identifiers
- Provides security protection against malformed multibyte sequences
- Uses fast path for ASCII characters and slower path for potential multibyte characters
- Unreserved keywords are not quoted, but all other keyword categories require quoting
- The returned string must be used immediately before any other formatting function calls