# raw_pg_bind_textdomain_codeset

## Location
[src/backend/utils/mb/mbutils.c:1187-1225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1187-L1225)

## Overview
Performs a single bind_textdomain_codeset() call to set the character encoding for a gettext message domain, translating a PostgreSQL encoding ID to a gettext codeset name.

## Definition
static bool raw_pg_bind_textdomain_codeset(const char *domainname, int encoding)

## Detailed Description
This internal static function wraps the gettext bind_textdomain_codeset() system call to configure the character encoding for a specific message domain. It translates PostgreSQL's internal encoding identifiers to gettext codeset names using the pg_enc2gettext_tbl[] translation table. The function handles error conditions gracefully and provides appropriate logging depending on the memory context availability.

The function fails for MULE_INTERNAL encoding (which is unknown to gettext) and can also fail due to gettext-internal issues such as out-of-memory conditions. Error reporting is context-aware - it uses elog() when a proper memory context is available, otherwise falls back to write_stderr() for direct stderr output.

## Parameters / Member Variables
- localdomain: The name of the gettext message domain to configure
- : PostgreSQL encoding identifier to be translated to gettext codeset

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_ENCODING (macro to validate encoding ID)
  - bind_textdomain_codeset (gettext library function)
  - elog (PostgreSQL logging function)
  - [write_stderr](../w/write_stderr.md) (direct stderr output function)
  - pg_enc2gettext_tbl[] (encoding translation table)
- Called from (representative examples):
  - [pg_bind_textdomain_codeset](../p/pg_bind_textdomain_codeset.md) (src/backend/utils/mb/mbutils.c:1238)
  - [pg_bind_textdomain_codeset](../p/pg_bind_textdomain_codeset.md) (src/backend/utils/mb/mbutils.c:1246)

## Notes and Other Information
- This is a static function internal to mbutils.c and not exposed in the public API
- The function is designed to be safe in both normal and bootstrap contexts by checking CurrentMemoryContext
- Returns true on success, false on failure
- MULE_INTERNAL encoding is explicitly unsupported as gettext doesn't recognize it
- Error logging is context-dependent to avoid memory allocation issues during early startup