# pg_mbcliplen

## Location
[src/backend/utils/mb/mbutils.c:1083-1092](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1083-L1092)

## Overview
Returns the byte length of a multibyte string up to a specified limit, ensuring multibyte character boundaries are not broken.

## Definition
```c
int pg_mbcliplen(const char *mbstr, int len, int limit)
```

## Detailed Description
`pg_mbcliplen` calculates the safe byte length of a multibyte string that does not exceed a specified limit while ensuring that multibyte character boundaries are preserved. This function is crucial when truncating strings to fit within specific byte constraints, such as database column limits or display boundaries.

The function serves as a wrapper around `pg_encoding_mbcliplen()`, delegating the actual work to the encoding-specific implementation. The key guarantee is that the returned length will never split a multibyte character - if including a character would exceed the limit, the function stops before that character rather than splitting it.

This is essential for maintaining data integrity in multibyte environments where splitting a character would create invalid byte sequences.

## Parameters / Member Variables
- `mbstr`: Pointer to the multibyte string to be clipped.
- `len`: The total byte length of the input string.
- `limit`: The maximum number of bytes allowed in the result.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_encoding_mbcliplen](pg_encoding_mbcliplen.md) (encoding-specific clipping implementation)
  - `DatabaseEncoding` (current database encoding information)
- Called from (representative examples):
  - [truncate_identifier](../t/truncate_identifier.md) (truncating SQL identifiers to valid lengths)
  - `namein` and `text_name` (converting strings to PostgreSQL name type)
  - [bpchar_name](../b/bpchar_name.md) (fixed-length character type operations)
  - [pgstat_clip_activity](pgstat_clip_activity.md) (truncating activity descriptions for statistics)
  - `text_to_cstring_buffer` (safe string buffer operations)
  - [write_syslog](../w/write_syslog.md) (truncating log messages)

## Notes and Other Information
- Returns a byte count that is safe to use for string truncation
- Guarantees that multibyte character boundaries are never broken
- Essential for preventing creation of invalid byte sequences when truncating
- Commonly used in identifier truncation, logging, and data conversion functions
- The returned length is always ≤ min(len, limit)
- Critical for maintaining data integrity in PostgreSQL's text processing infrastructure