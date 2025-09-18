# cvec

## Location
src/include/regex/regguts.h: 278 - 294

## Overview
A structure representing a set of characters in PostgreSQL's regex implementation, efficiently storing both individual characters and character ranges, with special support for locale-specific character classes.

## Definition
```c
struct cvec
{
    int         nchrs;          /* number of chrs */
    int         chrspace;       /* number of chrs allocated in chrs[] */
    chr        *chrs;           /* pointer to vector of chrs */
    int         nranges;        /* number of ranges (chr pairs) */
    int         rangespace;     /* number of ranges allocated in ranges[] */
    chr        *ranges;         /* pointer to vector of chr pairs */
    int         cclasscode;     /* value of "enum classes", or -1 */
};
```

## Detailed Description
The `cvec` (character vector) structure provides an efficient representation for sets of characters in PostgreSQL's regex engine. It can represent character sets in two ways: as individual characters stored in the `chrs` array, and as character ranges stored as min..max pairs in the `ranges` array.

For locale-specific character classes like [[:alpha:]], the structure has special behavior: the arrays contain only members of that class up to MAX_SIMPLE_CHR (inclusive), and `cclasscode` is set to the internal code for the character class rather than the default -1 used for ordinary character sets.

The structure is designed for memory efficiency. In cvecs created by `newcvec()` and freed by `freecvec()`, both character arrays are allocated immediately after the struct itself rather than being separately malloc'd, making `chrspace` and `rangespace` effectively immutable after creation.

## Parameters / Member Variables
- `nchrs`: Number of individual characters currently stored in the chrs array
- `chrspace`: Total number of character slots allocated in the chrs array
- `chrs`: Pointer to array of individual characters
- `nranges`: Number of character ranges currently stored (each range is a chr pair)
- `rangespace`: Total number of range slots allocated in the ranges array
- `ranges`: Pointer to array of character pairs representing ranges (min..max inclusive)
- `cclasscode`: Character class code from regc_locale.c for locale-specific classes, or -1 for ordinary character sets

## Dependencies
- Functions called/Symbols referenced:
  - [chr](chr.md) (character type used for individual characters and ranges)

- Called from (representative examples):
  - newcvec (creation and initialization)
  - addchr, addrange (character/range addition)
  - [freecvec](../f/freecvec.md) (cleanup)
  - [subcolorcvec](../s/subcolorcvec.md) (color operations)
  - [charclass](charclass.md), charclasscomplement (character class operations)
  - [element](../e/element.md), range, before, eclass (locale functions)
  - Various regex compilation functions

## Notes and Other Information
- Used extensively throughout PostgreSQL's regex compilation for representing character classes, bracket expressions, and character sets
- Optimized for memory efficiency with inline allocation of character arrays
- Supports both explicit character enumeration and range-based representation for compact storage
- Special handling for locale-dependent character classes enables efficient Unicode support
- The ranges array stores pairs of characters, so `ranges[0]` and `ranges[1]` form the first range, `ranges[2]` and `ranges[3]` form the second range, etc.
- Critical component for bracket expression parsing and character class resolution in regex patterns
- Part of the interface between the regex compiler and locale-specific character classification functions