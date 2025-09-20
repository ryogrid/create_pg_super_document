# pg_ctype_cache

## Location
[src/backend/regex/regc_pg_locale.c:708-714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L708-L714)

## Overview
A caching structure that stores character classification results for regular expression processing, optimizing repeated character class queries for specific collations and probe functions.

## Definition

```c
typedef struct pg_ctype_cache
{
	pg_wc_probefunc probefunc;	/* pg_wc_isalpha or a sibling */
	Oid			collation;		/* collation this entry is for */
	struct cvec cv;				/* cache entry contents */
	struct pg_ctype_cache *next;	/* chain link */
} pg_ctype_cache;
```
## Detailed Description
The  structure is a critical component of PostgreSQL's regular expression engine that provides efficient caching of character classification results. It stores the results of character class queries (such as "is this character alphabetic?") for specific combinations of probe functions and collations, avoiding expensive repeated calculations during regex matching.

This cache is implemented as a linked list where each entry corresponds to a specific probe function (like , , etc.) operating under a particular collation. The cache entries contain a  structure that holds the actual character sets and ranges that satisfy the probe function's criteria.

The caching mechanism is particularly important for performance when processing regular expressions with character classes like , , etc., as these require checking potentially thousands of Unicode code points against locale-specific classification rules.

## Parameters / Member Variables
- `probefunc`: Function pointer to a character classification function (e.g., , ) that determines which characters belong to a specific character class
- `collation`: The OID of the collation for which this cache entry is valid, ensuring locale-specific character classification
- `cv`: A  structure containing the cached results - arrays of individual characters and character ranges that satisfy the probe function
- `*next`: Pointer to the next cache entry in the linked list, enabling chaining of multiple cache entries
## Dependencies
- Functions called/Symbols referenced:
  -  (struct for storing character vectors)
  -  (function pointer typedef)
- Called from (representative examples):
  -  (at src/backend/regex/regc_pg_locale.c:768, 787)
  -  (at src/backend/regex/regc_pg_locale.c:722)

## Notes and Other Information
- The cache is organized as a singly-linked list accessible via 
- Cache entries are created dynamically by  when a specific probe function/collation combination is first encountered
- The  structure contains both individual characters ( array) and character ranges ( array) to efficiently represent large character sets
- Memory management is handled carefully with proper cleanup in case of allocation failures during cache population
- The cache persists for the duration of the regex compilation/execution session, providing significant performance benefits for complex patterns with multiple character classes
- Character classification is limited to  for performance reasons, with higher code points handled by runtime mechanisms