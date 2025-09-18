# fmtId

## Location
[src/fe_utils/string_utils.c:248-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L248-L262)

## Overview
A convenience wrapper function that formats and quotes PostgreSQL identifiers using the globally configured encoding setting.

## Definition
```c
const char *fmtId(const char *rawid)
```

## Detailed Description
This function provides a simplified interface to fmtIdEnc() by automatically using the encoding that was previously configured via setFmtEncoding(). It serves as the primary identifier formatting function used throughout PostgreSQL's frontend utilities when explicit encoding specification is not needed.

The function is essentially a thin wrapper that calls fmtIdEnc() with the current global encoding obtained from getFmtEncoding(). This design allows existing code to use identifier formatting without needing to track encoding information explicitly, while still ensuring proper multibyte character handling.

The function assumes that setFmtEncoding() has been called previously to configure the encoding. If the encoding has not been set, getFmtEncoding() will either trigger an assertion in debug builds or default to UTF-8 in production builds.

Like fmtIdEnc(), this function uses a shared buffer, so the returned string is only valid until the next call to any function that uses getLocalPQExpBuffer().

## Parameters / Member Variables
- `rawid`: The input identifier string to be formatted and potentially quoted

## Dependencies
- Functions called/Symbols referenced:
  - [fmtIdEnc](fmtIdEnc.md) (primary formatting function)
  - [getFmtEncoding](../g/getFmtEncoding.md) (to retrieve current encoding setting)
- Called from (representative examples):
  - Extensively used throughout pg_dump utilities (pg_dump.c, pg_dumpall.c, dumputils.c)
  - PostgreSQL client utilities (createdb.c, createuser.c, psql)
  - String utilities (appendReloptionsArray in string_utils.c)
  - Test modules (test_escape.c)

## Notes and Other Information
- Most commonly used identifier formatting function in PostgreSQL frontend code
- Requires prior call to setFmtEncoding() to configure the encoding
- Returns pointer to shared buffer - not thread-safe and not reentrant
- Recommended to use fmtIdEnc() directly when explicit encoding control is needed
- The returned string must be used immediately before any other formatting function calls
- Widely used across PostgreSQL's dump, restore, and administrative utilities
- Provides consistent identifier quoting behavior across all PostgreSQL frontend tools