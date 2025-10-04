# dump

## Location
[src/backend/regex/regcomp.c:2494-2556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2494-L2556)

## Overview
A debugging function that dumps the internal structure of a compiled regular expression in human-readable form.

## Definition

```c
struct guts *g;
```
## Detailed Description
The  function is a diagnostic utility within PostgreSQL's regex engine that outputs detailed information about a compiled regular expression's internal structure. It validates the regex structure's magic numbers, displays metadata, and recursively dumps various components including color maps, search NFAs, lookaround assertions, and the syntax tree. This function is primarily used for debugging and understanding regex compilation results.

## Parameters / Member Variables
- : Pointer to the compiled regular expression structure () to be dumped
- : File pointer where the dump output will be written

## Dependencies
- Functions called/Symbols referenced:
  - 
  -  
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
  - 
- Called from (representative examples):
  - Various debugging and diagnostic contexts in regex compilation

## Notes and Other Information
- This is a static function only accessible within regcomp.c
- Validates magic numbers for both the regex_t structure and its internal guts
- Dumps lookaround assertion information for positive/negative lookahead and lookbehind
- Part of PostgreSQL's internal regex debugging infrastructure
- Output format includes clear section headers and hierarchical structure representation

## Simplified Source

```c
static void dump(regex_t *re, FILE *f) {
    struct guts *g;
    int i;

    // Validate structure and output diagnostics
    if (re->re_magic != REMAGIC)
        fprintf(f, "bad magic number (0x%x not 0x%x)\n", re->re_magic, REMAGIC);
    if (re->re_guts == NULL) {
        fprintf(f, "NULL guts!!!\n");
        return;
    }

    g = (struct guts *) re->re_guts;
    if (g->magic != GUTSMAGIC)
        fprintf(f, "bad guts magic number (0x%x not 0x%x)\n", g->magic, GUTSMAGIC);

    // Output basic information
    fprintf(f, "\n\n\n========= DUMP ==========\n");
    fprintf(f, "nsub %d, info 0%lo, csize %d, ntree %d\n",
            (int) re->re_nsub, re->re_info, re->re_csize, g->ntree);

    // Dump color map and search NFA
    dumpcolors(&g->cmap, f);
    if (!NULLCNFA(g->search)) {
        fprintf(f, "\nsearch:\n");
        dumpcnfa(&g->search, f);
    }

    // Dump lookaround constraints
    for (i = 1; i < g->nlacons; i++) {
        struct subre *lasub = &g->lacons[i];
        const char *latype = "???";

        switch (lasub->latype) {
            case LATYPE_AHEAD_POS: latype = "positive lookahead"; break;
            case LATYPE_AHEAD_NEG: latype = "negative lookahead"; break;
            case LATYPE_BEHIND_POS: latype = "positive lookbehind"; break;
            case LATYPE_BEHIND_NEG: latype = "negative lookbehind"; break;
        }

        fprintf(f, "\nla%d (%s):\n", i, latype);
        dumpcnfa(&lasub->cnfa, f);
    }

    // Dump syntax tree
    fprintf(f, "\n");
    dumpst(g->tree, f, 0);
}
```