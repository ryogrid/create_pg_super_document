# pg_re_flags

## Location
[src/backend/utils/adt/regexp.c:45-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L45-L49)

## Overview
The pg_re_flags structure encapsulates all the options of interest for PostgreSQL's regular expression functions, providing a consolidated way to pass compilation flags and behavioral options.

## Definition

```c
typedef struct pg_re_flags
{
	int			cflags;			/* compile flags for Spencer's regex code */
	bool		glob;			/* do it globally (for each occurrence) */
} pg_re_flags;
```
## Detailed Description
The pg_re_flags structure is used throughout PostgreSQL's regular expression system to standardize the passing of compilation and execution options. It serves as a parameter container that combines Spencer's regex compilation flags with PostgreSQL-specific behavioral flags. This structure ensures consistent handling of regex options across all regex-related functions in the backend.

## Parameters / Member Variables
- `cflags`: Integer containing compile flags that are passed directly to Spencer's regex code, controlling pattern compilation behavior such as case sensitivity, extended syntax, etc.
- `glob`: Boolean flag indicating whether the regular expression operation should be performed globally (i.e., for each occurrence in the string rather than just the first match)
## Dependencies
- Functions called/Symbols referenced:
  - (This is a simple struct definition with no direct function calls)
- Called from (representative examples):
  - [parse_re_flags](parse_re_flags.md)
  - [textregexreplace](../t/textregexreplace.md)
  - [textregexreplace_extended](../t/textregexreplace_extended.md)
  - [regexp_count](../r/regexp_count.md)
  - [regexp_instr](../r/regexp_instr.md)
  - [regexp_like](../r/regexp_like.md)
  - [regexp_match](../r/regexp_match.md)
  - [regexp_matches](../r/regexp_matches.md)
  - [setup_regexp_matches](../s/setup_regexp_matches.md)
  - [regexp_split_to_table](../r/regexp_split_to_table.md)
  - [regexp_split_to_array](../r/regexp_split_to_array.md)
  - [regexp_substr](../r/regexp_substr.md)

## Notes and Other Information
This structure is fundamental to PostgreSQL's regex implementation and is used extensively throughout src/backend/utils/adt/regexp.c. The separation of Spencer's regex flags (cflags) from PostgreSQL's own behavioral flags (glob) provides clean abstraction and allows for easy extension of regex functionality. The structure is typically populated by parse_re_flags() function based on user-provided flag strings.