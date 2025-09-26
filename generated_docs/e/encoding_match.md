# encoding_match

## Location
src/port/chklocale.c: 39 - 201

## Overview
The  structure defines a mapping between PostgreSQL internal encoding identifiers and their corresponding system-specific encoding names, enabling conversion between different character encoding representations across various platforms.

## Definition

```c
struct encoding_match
{
	enum pg_enc pg_enc_code;
	const char *system_enc_name;
};
```
## Detailed Description
The  structure serves as a fundamental component in PostgreSQL's character encoding management system. It provides a bidirectional mapping table that allows the system to:

1. **Translate system encoding names to PostgreSQL encoding IDs**: When PostgreSQL needs to determine which internal encoding corresponds to a system-provided encoding name (from  on Unix systems or codepage numbers on Windows).

2. **Support cross-platform encoding recognition**: The structure accommodates various spelling conventions and platform-specific encoding names (e.g., "EUC-JP", "eucJP", "IBM-eucJP", "CP20932" all map to ).

3. **Enable locale-based encoding detection**: Used primarily in  and  functions to automatically determine appropriate character encodings based on system locale settings.

The structure is instantiated as a static array  containing comprehensive mappings for all supported PostgreSQL encodings, including both backend and frontend-only encodings. The table is searched using case-insensitive comparison (), making it robust against capitalization variants.

## Member Variables
- : PostgreSQL internal encoding identifier from the  enumeration (e.g., , , )
- : System-specific encoding name as a null-terminated string (e.g., "UTF-8", "ISO-8859-1", "CP932")

## Dependencies
- Functions using this structure:
  -  (src/port/chklocale.c:306)
  -  (src/port/chklocale.c:270, Windows only)
- Referenced symbols:
  -  enum (src/include/mb/pg_wchar.h:240)
  -  for case-insensitive string comparison
  - All PostgreSQL encoding constants (PG_UTF8, PG_LATIN1, PG_SJIS, etc.)

## Notes and Other Information
- The encoding match table includes entries for codepage numbers (CPnnn format) specifically for Windows platform compatibility
- The table supports multiple alternative names for the same encoding to handle different system conventions and historical variations
- The table is terminated with a NULL entry () to mark the end of the array
- Case variations don't require separate entries since searches use 
- This structure is critical for initdb to recognize encoding mismatches and for proper locale-based database initialization
- The design accommodates both backend encodings (server-side) and frontend-only encodings (client-side only)