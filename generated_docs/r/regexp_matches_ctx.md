# regexp_matches_ctx

## Location
[src/backend/utils/adt/regexp.c:52-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L52-L67)

## Overview
The regexp_matches_ctx structure maintains cross-call state for PostgreSQL's regexp_match and regexp_split functions, enabling efficient processing of multiple matches within a single string.

## Definition

```c
typedef struct regexp_matches_ctx
{
	text	   *orig_str;		/* data string in original TEXT form */
	int			nmatches;		/* number of places where pattern matched */
	int			npatterns;		/* number of capturing subpatterns */
	/* We store start char index and end+1 char index for each match */
	/* so the number of entries in match_locs is nmatches * npatterns * 2 */
	int		   *match_locs;		/* 0-based character indexes */
	int			next_match;		/* 0-based index of next match to process */
	/* workspace for build_regexp_match_result() */
	Datum	   *elems;			/* has npatterns elements */
	bool	   *nulls;			/* has npatterns elements */
	pg_wchar   *wide_str;		/* wide-char version of original string */
	char	   *conv_buf;		/* conversion buffer, if needed */
	int			conv_bufsiz;	/* size thereof */
} regexp_matches_ctx;
```
## Detailed Description
This structure serves as a context container for set-returning functions that process regular expression matches. It maintains all necessary state information between function calls, including the original string, match locations, and workspace buffers. The structure is designed to handle multiple matches efficiently by pre-computing all match positions and then returning them one by one in subsequent function calls. It also handles character encoding conversions and provides workspace for constructing result arrays.

## Parameters / Member Variables
- `*orig_str`: Pointer to the original input string in PostgreSQL's TEXT format
- `nmatches`: Total number of locations where the regular expression pattern matched in the string
- `npatterns`: Number of capturing subpatterns (parenthesized groups) in the regular expression
- `*match_locs`: Array storing start and end+1 character indexes for each match and subpattern (size: nmatches * npatterns * 2)
- `next_match`: Index of the next match to be processed and returned (0-based)
- `*elems`: Workspace array for constructing result tuples, with npatterns elements
- `*nulls`: Boolean array indicating null values in result tuples, with npatterns elements
- `*wide_str`: Wide-character (pg_wchar) version of the original string for proper Unicode handling
- `*conv_buf`: Buffer used for character encoding conversions when needed
- `conv_bufsiz`: Size of the conversion buffer in bytes
## Dependencies
- Functions called/Symbols referenced:
  - [text](../t/text.md) (PostgreSQL TEXT type)
  - Datum (PostgreSQL datum type)
  - pg_wchar (PostgreSQL wide character type)
- Called from (representative examples):
  - [regexp_count](regexp_count.md)
  - [regexp_instr](regexp_instr.md)  
  - [regexp_match](regexp_match.md)
  - [regexp_matches](regexp_matches.md)
  - [regexp_matches_no_flags](regexp_matches_no_flags.md)
  - [setup_regexp_matches](../s/setup_regexp_matches.md)
  - [build_regexp_match_result](../b/build_regexp_match_result.md)
  - [regexp_split_to_table](regexp_split_to_table.md)
  - [regexp_split_to_array](regexp_split_to_array.md)
  - [build_regexp_split_result](../b/build_regexp_split_result.md)
  - [regexp_substr](regexp_substr.md)

## Notes and Other Information
This structure is critical for the efficient implementation of PostgreSQL's set-returning regular expression functions. By pre-computing all matches and storing them in the context, the system avoids repeated regex execution for each returned row. The structure handles both simple matching and complex scenarios with multiple capturing groups. Memory management for this structure is handled through PostgreSQL's memory context system, ensuring proper cleanup when the function completes.