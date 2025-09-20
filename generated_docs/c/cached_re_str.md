# cached_re_str

## Location
[src/backend/utils/adt/regexp.c:102-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L102-L110)

## Overview
The cached_re_str structure describes one cached regular expression in PostgreSQL's regex caching system, storing both the compiled regex and its associated metadata for efficient reuse.

## Definition

```c
typedef struct cached_re_str
{
	MemoryContext cre_context;	/* memory context for this regexp */
	char	   *cre_pat;		/* original RE (not null terminated!) */
	int			cre_pat_len;	/* length of original RE, in bytes */
	int			cre_flags;		/* compile flags: extended,icase etc */
	Oid			cre_collation;	/* collation to use */
	regex_t		cre_re;			/* the compiled regular expression */
} cached_re_str;
```
## Detailed Description
This structure implements PostgreSQL's regular expression caching mechanism, which stores compiled regular expressions to avoid repeated compilation overhead. Each cached entry contains the original pattern, compilation parameters, and the compiled regex object from Spencer's regex library. The cache uses memory contexts for proper memory management and tracks collation information to ensure cached patterns are reused only when appropriate. This caching significantly improves performance for repeated regex operations with the same patterns.

## Parameters / Member Variables
- : Memory context allocated specifically for this cached regular expression, enabling proper memory management and cleanup
- : Pointer to the original regular expression pattern string (note: not null-terminated)
- : Length of the original regular expression pattern in bytes
- : Compilation flags passed to the regex engine, including options like extended syntax, case insensitive matching, etc.
- : Object identifier (Oid) specifying the collation to use for this regular expression
- : The actual compiled regular expression object (regex_t type from Spencer's regex library)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContext](../M/MemoryContext.md) (PostgreSQL memory context type)
  - Oid (PostgreSQL object identifier type)
  - regex_t (Spencer's regex library compiled expression type)
- Called from (representative examples):
  - [RE_compile_and_cache](../R/RE_compile_and_cache.md) (multiple references for cache management)

## Notes and Other Information
This structure is fundamental to PostgreSQL's regex performance optimization strategy. The caching system maintains a small number of recently used compiled regular expressions to avoid the overhead of recompilation. The structure carefully tracks collation information because the same pattern text may behave differently under different collations. The use of PostgreSQL's memory context system ensures that cached regex objects are properly cleaned up when no longer needed. The pattern is stored without null termination as an optimization, requiring the length field for proper handling.