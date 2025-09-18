# vars

## Location
src/backend/regex/regcomp.c: 281 - 312

## Overview
The `vars` struct is a central data structure in PostgreSQL's regex engine that bundles all internal variables and state needed during regular expression compilation and execution, providing a convenient way to pass context between regex processing functions.

## Definition
```c
struct vars
{
    regex_t    *re;                 /* compiled regex structure */
    const chr  *now;                /* scan pointer into string */
    const chr  *stop;               /* end of string */
    int         err;                /* error code (0 if none) */
    int         cflags;             /* copy of compile flags */
    int         lasttype;           /* type of previous token */
    int         nexttype;           /* type of next token */
    chr         nextvalue;          /* value (if any) of next token */
    int         lexcon;             /* lexical context type (see regc_lex.c) */
    int         nsubexp;            /* subexpression count */
    struct subre **subs;            /* subRE pointer vector */
    size_t      nsubs;              /* length of vector */
    struct subre *sub10[10];        /* initial vector, enough for most */
    struct nfa *nfa;                /* the NFA */
    struct colormap *cm;            /* character color map */
    color       nlcolor;            /* color of newline */
    struct state *wordchrs;         /* state in nfa holding word-char outarcs */
    struct subre *tree;             /* subexpression tree */
    struct subre *treechain;        /* all tree nodes allocated */
    struct subre *treefree;         /* any free tree nodes */
    int         ntree;              /* number of tree nodes, plus one */
    struct cvec *cv;                /* interface cvec */
    struct cvec *cv2;               /* utility cvec */
    struct subre *lacons;           /* lookaround-constraint vector */
    int         nlacons;            /* size of lacons[]; note that only slots numbered 1 .. nlacons-1 are used */
    size_t      spaceused;          /* approx. space used for compilation */
};
```

## Detailed Description
The `vars` struct serves as the primary context holder for PostgreSQL's regex compilation engine, located in `src/backend/regex/regcomp.c:281-312`. It encapsulates all the state information, intermediate data structures, and configuration needed throughout the regex compilation process. This design allows the regex engine to pass a single pointer rather than numerous individual parameters between the many internal functions involved in parsing, compiling, and optimizing regular expressions.

The struct is extensively used throughout the regex subsystem including lexical analysis (`regc_lex.c`), NFA construction (`regc_nfa.c`), character classification (`regc_locale.c`), color mapping (`regc_color.c`), and execution (`regexec.c`). It maintains both the input state (current position, flags) and the growing output structures (NFA, subexpression tree, character maps).

## Parameters / Member Variables
- `re`: Points to the output `regex_t` structure being built
- `now`: Current scanning position in the input regular expression string
- `stop`: Pointer to the end of the input string
- `err`: Error code accumulator (0 indicates no error)
- `cflags`: Copy of compilation flags passed to the regex compiler
- `lasttype`: Token type of the previously processed lexical token
- `nexttype`: Token type of the next token to be processed
- `nextvalue`: Character value associated with the next token (if applicable)
- `lexcon`: Current lexical context for the scanner (defined in regc_lex.c)
- `nsubexp`: Count of subexpressions (capturing groups) found in the regex
- `subs`: Dynamic array of pointers to subexpression structures
- `nsubs`: Current allocated size of the subs array
- `sub10[10]`: Initial static storage for subexpressions (optimization for common case)
- `nfa`: Pointer to the NFA (Non-deterministic Finite Automaton) being constructed
- `cm`: Character color map for efficient character class handling
- `nlcolor`: Special color assigned to newline characters
- `wordchrs`: NFA state containing word character transition arcs
- `tree`: Root of the subexpression parse tree
- `treechain`: Linked list of all allocated tree nodes for cleanup
- `treefree`: Free list of available tree nodes for reuse
- `ntree`: Total count of tree nodes allocated plus one
- `cv`: Primary character vector interface for character set operations
- `cv2`: Secondary character vector for utility operations
- `lacons`: Array of lookaround constraint structures
- `nlacons`: Size of the lacons array (slots 1 to nlacons-1 are used)
- `spaceused`: Approximate memory consumption tracking for compilation

## Dependencies
- Functions called/Symbols referenced:
  - regex_t (target regex structure type)
  - chr (character type used throughout regex engine)
  - subre (subexpression structure)
  - colormap (character color mapping structure)
  - color (character color type)
  - state (NFA state structure)
  - cvec (character vector structure)
- Called from (representative examples):
  - pg_regcomp (main regex compilation entry point)
  - parse (recursive descent parser functions)
  - lexstart, next, lexescape (lexical analysis functions)
  - Various regex execution functions in regexec.c

## Notes and Other Information
The `vars` struct is typically allocated on the stack in the main compilation function and passed by pointer to all subordinate functions. This approach provides efficient context passing while maintaining clear separation of concerns. The struct includes both small optimizations (like the `sub10` static array) and comprehensive memory tracking (`spaceused`). The dual character vector design (`cv` and `cv2`) allows for efficient temporary operations without constant allocation/deallocation. The `treechain` and `treefree` members implement a custom memory management system for parse tree nodes, enabling both efficient allocation and proper cleanup on error conditions.