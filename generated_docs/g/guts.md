# guts

## Location
[src/include/regex/regguts.h:530-532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/regex/regguts.h#L530-L532)

## Overview
The guts struct contains the internal implementation details of a compiled regular expression in PostgreSQL, serving as the hidden implementation behind the public pg_regex_t interface.

## Definition
```c
struct guts
{
    int         magic;
    int         cflags;         /* copy of compile flags */
    long        info;           /* copy of re_info */
    size_t      nsub;           /* copy of re_nsub */
    struct subre *tree;
    struct cnfa search;         /* for fast preliminary search */
    int         ntree;          /* number of subre's, plus one */
    struct colormap cmap;
    int         FUNCPTR(compare, (const chr *, const chr *, size_t));
    struct subre *lacons;       /* lookaround-constraint vector */
    int         nlacons;        /* size of lacons[]; note that only slots
                                 * numbered 1 .. nlacons-1 are used */
};
```

## Detailed Description
The guts struct is the core internal representation of a compiled regular expression in PostgreSQL's regex engine. It is hidden behind a void pointer (re_guts field) in the public pg_regex_t structure to maintain API encapsulation. This struct contains all the compiled regex data structures needed for pattern matching, including the syntax tree representation, character mapping information, and optimization structures for fast searching. The magic number (GUTSMAGIC = 0xfed9) is used for validation and debugging purposes.

## Parameters / Member Variables
- `magic`: Magic number (0xfed9) used for structure validation and debugging
- `cflags`: Copy of the compilation flags passed when the regex was compiled
- `info`: Copy of the re_info bitmask containing various regex properties and capabilities
- `nsub`: Copy of re_nsub indicating the number of subexpressions in the regex
- `tree`: Pointer to the root of the syntax tree representation of the compiled regex
- `search`: CNFA (Compiled Nondeterministic Finite Automaton) structure for fast preliminary search
- `ntree`: Total number of subre (subexpression) structures in the tree, plus one
- `cmap`: Color map structure that maps characters to equivalence classes for efficient matching
- `compare`: Function pointer for character comparison operations, supports different collations
- `lacons`: Array of lookaround constraint structures for lookahead/lookbehind assertions
- `nlacons`: Size of the lacons array, with valid entries from index 1 to nlacons-1

## Dependencies
- Functions called/Symbols referenced:
  - subre (subexpression tree nodes)
  - [cnfa](../c/cnfa.md) (compiled NFA structure)
  - [colormap](../c/colormap.md) (character classification structure)
- Called from (representative examples):
  - pg_regcomp (in regcomp.c:380) - allocates and initializes guts during compilation
  - [rfree](../r/rfree.md) (in regcomp.c:2449, 2455) - [cleanup](../c/cleanup.md) and deallocation
  - [vars](../v/vars.md) (in regexec.c:109) - execution context references guts
  - pg_reg_* functions (in regexport.c) - various export functions access guts internals
  - [pg_regprefix](../p/pg_regprefix.md) (in regprefix.c:50, 68) - prefix optimization functions

## Notes and Other Information
- The guts struct is always accessed through a char* pointer (re_guts) in pg_regex_t for portability
- Magic number validation helps detect memory corruption and invalid regex_t objects  
- The structure supports advanced regex features like lookaround constraints and complex subexpression trees
- Character color mapping is a key optimization that reduces the character set to manageable equivalence classes
- The compare function pointer allows for locale-aware and collation-specific character comparisons
- Lookaround constraints (lacons) support zero-width assertions like (?=...) and (?<!...)
- This internal structure is not exposed to external users, maintaining a clean public API