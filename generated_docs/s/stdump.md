# stdump

## Location
src/backend/regex/regcomp.c: 2572 - 2629

## Overview
A recursive helper function that dumps the syntax tree structure of a compiled regular expression subexpression in human-readable form.

## Definition
```c
static void stdump(struct subre *t, FILE *f, int nfapresent)
```

## Detailed Description
The `stdump` function is the recursive implementation core of the `dumpst` function in PostgreSQL's regex engine. It traverses and prints detailed information about each node in the regular expression syntax tree, including operation type, flags, quantification bounds, capture groups, backreferences, and structural relationships. The function recursively processes child and sibling nodes to provide a complete hierarchical view of the regex structure.

## Parameters / Member Variables
- `t`: Pointer to the syntax tree node (`struct subre`) to be dumped
- `f`: File pointer where the dump output will be written
- `nfapresent`: Flag indicating whether the original NFA structure is still available for displaying node ranges

## Dependencies
- Functions called/Symbols referenced:
  - `subre`
  - `stid`
  - `LONGER`
  - `SHORTER` 
  - `MIXED`
  - `CAP`
  - `BACKR`
  - `BRUSE`
  - `INUSE`
  - `DUPINF`
  - `NULLCNFA`
  - `dumpcnfa`
- Called from (representative examples):
  - `dumpst`
  - `stdump` (recursive self-call)

## Notes and Other Information
- This is a static function only accessible within regcomp.c
- Displays various regex node flags including longest/shortest match preferences, capture status, and backref usage
- Shows quantification bounds in {min,max} format where applicable
- Recursively dumps child and sibling nodes to show complete tree structure
- When NFA is present, displays node range information for debugging
- Part of PostgreSQL's internal regex debugging infrastructure