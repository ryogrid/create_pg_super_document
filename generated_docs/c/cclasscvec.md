# cclasscvec

## Location
[src/backend/regex/regc_locale.c:569-670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_locale.c#L569-L670)

## Overview
The cclasscvec function creates character vectors (cvecs) for POSIX character classes like [:alpha:], [:digit:], etc., with optional case-independent matching support in PostgreSQL's regular expression engine.

## Definition

```c
static struct cvec *
cclasscvec(struct vars *v,		/* context */
		   enum char_classes cclasscode,	/* class to build a cvec for */
		   int cases)			/* case-independent? */
```
## Detailed Description
The cclasscvec function is the central implementation for POSIX character class support in PostgreSQL's regex engine. It translates character class identifiers into concrete character sets, handling both locale-dependent and hard-wired character class definitions.

The function employs two different strategies for character class implementation:

1. **Cached locale-dependent classes**: For classes based on standard C library functions (isalpha, isdigit, etc.), it uses pg_ctype_get_cache() to obtain cached character vectors that respect the current locale settings. This approach provides proper internationalization support.

2. **Hard-wired classes**: For classes with well-defined meanings independent of locale (ASCII, BLANK, CNTRL, XDIGIT), it constructs character vectors directly using specific character ranges or individual characters.

**Case handling**: When case-independent matching is requested, the function remaps CC_LOWER and CC_UPPER to CC_ALPHA, since in case-insensitive mode, lowercase and uppercase characters should be treated as equivalent to all alphabetic characters.

The function supports all standard POSIX character classes: alnum, alpha, blank, cntrl, digit, graph, lower, print, punct, space, upper, word, xdigit, and ascii.

## Parameters / Member Variables
- : Context structure containing regex compilation state and error handling
- : Enumerated value identifying which character class to generate (CC_ALPHA, CC_DIGIT, etc.)
- : Flag indicating case-independent matching (non-zero remaps lower/upper to alpha)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ctype_get_cache](../p/pg_ctype_get_cache.md) (retrieve cached locale-aware character sets)
  - getcvec (allocate new character vector)
  - addrange (add character range to cvec)
  - addchr (add individual character to cvec)
  - [pg_wc_isprint](../p/pg_wc_isprint.md), pg_wc_isalnum, pg_wc_isalpha, etc. (character classification functions)
  - ERR (error reporting macro)
  - REG_ESPACE (out of memory error code)
  - CC_* constants (character class identifiers)
- Called from (representative examples):
  - [charclass](charclass.md) (in regcomp.c:1503)
  - [charclasscomplement](charclasscomplement.md) (in regcomp.c:1532)
  - [wordchrs](../w/wordchrs.md) (in regcomp.c:2007)

## Notes and Other Information
- Returns either cached cvecs (for locale-dependent classes) or transient cvecs (for hard-wired classes)
- Callers should not explicitly free the returned cvec as it may be cached
- The cclass_column_index() function must be kept in sync with this implementation
- Hard-wired XDIGIT definition uses ASCII hex digits regardless of locale
- Provides comprehensive POSIX character class support for PostgreSQL's regex implementation
- Memory allocation failure results in REG_ESPACE error

## Simplified Source

```c
static struct cvec *cclasscvec(struct vars *v,
                              enum char_classes cclasscode,
                              int cases)
{
    struct cvec *cv = NULL;

    // Remap lower/upper to alpha for case-insensitive matching
    if (cases && (cclasscode == CC_LOWER || cclasscode == CC_UPPER))
        cclasscode = CC_ALPHA;

    // Handle different character classes
    switch (cclasscode) {
        // Locale-dependent classes (cached)
        case CC_PRINT:
            cv = pg_ctype_get_cache(pg_wc_isprint, cclasscode);
            break;
        case CC_ALNUM:
            cv = pg_ctype_get_cache(pg_wc_isalnum, cclasscode);
            break;
        case CC_ALPHA:
            cv = pg_ctype_get_cache(pg_wc_isalpha, cclasscode);
            break;
        case CC_WORD:
            cv = pg_ctype_get_cache(pg_wc_isword, cclasscode);
            break;
        case CC_DIGIT:
            cv = pg_ctype_get_cache(pg_wc_isdigit, cclasscode);
            break;
        case CC_PUNCT:
            cv = pg_ctype_get_cache(pg_wc_ispunct, cclasscode);
            break;
        case CC_SPACE:
            cv = pg_ctype_get_cache(pg_wc_isspace, cclasscode);
            break;
        case CC_LOWER:
            cv = pg_ctype_get_cache(pg_wc_islower, cclasscode);
            break;
        case CC_UPPER:
            cv = pg_ctype_get_cache(pg_wc_isupper, cclasscode);
            break;
        case CC_GRAPH:
            cv = pg_ctype_get_cache(pg_wc_isgraph, cclasscode);
            break;

        // Hard-wired classes
        case CC_ASCII:
            cv = getcvec(v, 0, 1);
            if (cv)
                addrange(cv, 0, 0x7f);
            break;
        case CC_BLANK:
            cv = getcvec(v, 2, 0);
            addchr(cv, '\t');
            addchr(cv, ' ');
            break;
        case CC_CNTRL:
            cv = getcvec(v, 0, 2);
            addrange(cv, 0x0, 0x1f);
            addrange(cv, 0x7f, 0x9f);
            break;
        case CC_XDIGIT:
            cv = getcvec(v, 0, 3);
            if (cv) {
                addrange(cv, '0', '9');
                addrange(cv, 'a', 'f');
                addrange(cv, 'A', 'F');
            }
            break;
    }

    // Handle memory allocation failure
    if (cv == NULL)
        ERR(REG_ESPACE);

    return cv;
}
```