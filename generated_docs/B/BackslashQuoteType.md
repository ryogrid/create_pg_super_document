# BackslashQuoteType

## Location
[src/include/parser/parser.h:53-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/parser/parser.h#L53-L68)

## Overview
BackslashQuoteType is an enumeration that defines the allowed values for the backslash_quote GUC (Grand Unified Configuration) parameter, which controls whether backslash-quote sequences (\\') are allowed in string literals.

## Definition

```c
typedef enum
{
	BACKSLASH_QUOTE_OFF,
	BACKSLASH_QUOTE_ON,
	BACKSLASH_QUOTE_SAFE_ENCODING,
}			BackslashQuoteType;
```
## Detailed Description
This enumeration serves as the type definition for the PostgreSQL GUC parameter `backslash_quote`, which determines how the parser handles backslash-quote sequences (\\') in string literals. The parameter provides compatibility options for applications that may rely on non-standard SQL behavior regarding escape sequences in string literals. This setting is particularly important for applications migrating from other database systems or legacy PostgreSQL configurations.

The enum is used internally by the lexical scanner and parser to enforce the configured behavior when processing string literals containing backslash-quote sequences.

## Parameters / Member Variables
- `BACKSLASH_QUOTE_OFF`: Completely disables backslash-quote sequences in string literals, treating them as an error
- `BACKSLASH_QUOTE_ON`: Allows backslash-quote sequences in string literals unconditionally  
- `BACKSLASH_QUOTE_SAFE_ENCODING`: Allows backslash-quote sequences only when using a safe encoding (excludes client-only encodings)

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a standalone enum definition)
- Used by:
  - `backslash_quote` global variable in src/backend/parser/scan.l
  - Configuration system in src/backend/utils/misc/guc_tables.c
  - Lexical scanner logic for string literal processing

## Notes and Other Information
- This enum is part of the PostgreSQL GUC system for backward compatibility
- The default value is BACKSLASH_QUOTE_SAFE_ENCODING
- The setting affects SQL standard compliance - BACKSLASH_QUOTE_OFF provides the most standard-compliant behavior
- Client-only encodings (like SQL_ASCII) are considered unsafe for BACKSLASH_QUOTE_SAFE_ENCODING mode
- This parameter is user-settable (PGC_USERSET) and can be changed within a session
- The comment in the header file ("every one of these is a bad idea :-(") reflects the PostgreSQL developers' preference for standard-compliant SQL parsing