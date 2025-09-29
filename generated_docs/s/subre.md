# subre

## Location
[src/backend/regex/regcomp.c:2095-2151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2095-L2151)

## Overview
Allocates and initializes a new subre (sub-regular expression) structure, which represents a node in the parse tree of a regular expression during compilation.

## Definition

```c
*/
static struct subre *
subre(struct vars *v,
	  int op,
	  int flags,
	  struct state *begin,
	  struct state *end)
```
## Detailed Description
The subre function creates and initializes a new subre structure, which represents a node in the regular expression parse tree. It implements a memory management optimization by maintaining a free list of previously allocated subre structures for reuse, and includes stack overflow protection to prevent infinite recursion during parsing.

The function handles memory allocation in two ways:
1. Reuses structures from the free list (v->treefree) when available
2. Allocates new memory when the free list is empty, linking it to a chain for later cleanup

Each subre structure represents a specific operation or construct in the regular expression (indicated by the op parameter) and maintains pointers to the corresponding NFA states that implement that construct. The structure is initialized with sensible defaults and will have its specific fields updated by the calling code as needed.

## Parameters / Member Variables
- : Pointer to vars structure containing regex compilation state and memory management pointers
- : Character indicating the operation type (must be one of "=b|.*(" according to assertion)
- : Integer flags controlling behavior of this subre node
- : Pointer to the starting NFA state for this sub-expression
- : Pointer to the ending NFA state for this sub-expression

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP (macro to check for stack overflow)
  - ERR (error reporting macro)
  - MALLOC (memory allocation macro)
  - ZAPCNFA (macro to initialize cnfa structure)
- Constants used:
  - REG_ETOOBIG (regex too complex error)
  - REG_ESPACE (out of memory error)
- Data structures used:
  - [subre](subre.md) (sub-regular expression structure)
  - [state](state.md) (NFA state structure)
  - [cnfa](../c/cnfa.md) (compiled NFA structure)
- Called from (representative examples):
  - [parse](../p/parse.md) functions throughout regcomp.c
  - [newlacon](../n/newlacon.md) function (regcomp.c:2397-2409)
  - Various ARCV macro expansions

## Notes and Other Information
- Implements memory pool optimization through treefree linked list for performance
- Includes stack overflow protection to prevent infinite recursion during complex regex parsing
- All subre structures are chained together via treechain for cleanup purposes
- The op field must be one of the valid operation characters as enforced by assertion
- Initial field values: latype=-1, id=0, capno=0, backno=0, min=max=1, child=NULL, sibling=NULL
- The cnfa field is initialized with ZAPCNFA and will be populated later during compilation
- Memory allocation failures are handled gracefully with proper error reporting
- Used extensively throughout the regex parsing and compilation process to build the parse tree

## Simplified Source

```c
static struct subre *
subre(struct vars *v, int op, int flags, struct state *begin, struct state *end)
{
    struct subre *ret;

    // Check for stack overflow to prevent infinite recursion
    if (STACK_TOO_DEEP(v->re)) {
        ERR(REG_ETOOBIG);
        return NULL;
    }

    // Try to reuse from free list first, otherwise allocate new
    if (v->treefree != NULL) {
        ret = v->treefree;
        v->treefree = ret->child;
    } else {
        ret = (struct subre *) MALLOC(sizeof(struct subre));
        if (ret == NULL) {
            ERR(REG_ESPACE);
            return NULL;
        }
        // Chain for cleanup
        ret->chain = v->treechain;
        v->treechain = ret;
    }

    // Initialize the subre structure with defaults
    ret->op = op;
    ret->flags = flags;
    ret->latype = -1;
    ret->id = 0;
    ret->capno = 0;
    ret->backno = 0;
    ret->min = ret->max = 1;
    ret->child = NULL;
    ret->sibling = NULL;
    ret->begin = begin;
    ret->end = end;
    ZAPCNFA(ret->cnfa);

    return ret;
}
```