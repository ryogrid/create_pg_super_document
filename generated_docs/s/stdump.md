# stdump

## Location
[src/backend/regex/regcomp.c:2572-2629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2572-L2629)

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
  - `[subre](subre.md)`
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
  - [dumpcnfa](../d/dumpcnfa.md)
- Called from (representative examples):
  - `dumpst`
  - [stdump](stdump.md) (recursive self-call)

## Notes and Other Information
- This is a static function only accessible within regcomp.c
- Displays various regex node flags including longest/shortest match preferences, capture status, and backref usage
- Shows quantification bounds in {min,max} format where applicable
- Recursively dumps child and sibling nodes to show complete tree structure
- When NFA is present, displays node range information for debugging
- Part of PostgreSQL's internal regex debugging infrastructure

## Simplified Source

```c
static void stdump(struct subre *t, FILE *f, int nfapresent) {
    char idbuf[50];
    struct subre *t2;

    // Print basic node information
    fprintf(f, "%s. `%c'", stid(t, idbuf, sizeof(idbuf)), t->op);

    // Print flags
    if (t->flags & LONGER) fprintf(f, " longest");
    if (t->flags & SHORTER) fprintf(f, " shortest");
    if (t->flags & MIXED) fprintf(f, " hasmixed");
    if (t->flags & CAP) fprintf(f, " hascapture");
    if (t->flags & BACKR) fprintf(f, " hasbackref");
    if (t->flags & BRUSE) fprintf(f, " isreferenced");
    if (!(t->flags & INUSE)) fprintf(f, " UNUSED");

    // Print special attributes
    if (t->latype != (char) -1) fprintf(f, " latype(%d)", t->latype);
    if (t->capno != 0) fprintf(f, " capture(%d)", t->capno);
    if (t->backno != 0) fprintf(f, " backref(%d)", t->backno);

    // Print quantifiers
    if (t->min != 1 || t->max != 1) {
        fprintf(f, " %d,", t->min);
        if (t->max != DUPINF) fprintf(f, "%d", t->max);
        fprintf(f, "}");
    }

    // Print relationships and NFA info
    if (nfapresent) fprintf(f, " %ld-%ld", (long) t->begin->no, (long) t->end->no);
    if (t->child != NULL) fprintf(f, " C:%s", stid(t->child, idbuf, sizeof(idbuf)));
    if (t->child != NULL && t->child->sibling != NULL)
        fprintf(f, " C2:%s", stid(t->child->sibling, idbuf, sizeof(idbuf)));
    if (t->sibling != NULL) fprintf(f, " S:%s", stid(t->sibling, idbuf, sizeof(idbuf)));

    // Dump compiled NFA if present
    if (!NULLCNFA(t->cnfa)) {
        fprintf(f, "\n");
        dumpcnfa(&t->cnfa, f);
    }

    fprintf(f, "\n");

    // Recursively dump children
    for (t2 = t->child; t2 != NULL; t2 = t2->sibling)
        stdump(t2, f, nfapresent);
}
```
