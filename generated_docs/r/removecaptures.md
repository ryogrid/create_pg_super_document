# removecaptures

## Location
[src/backend/regex/regcomp.c:2218-2263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2218-L2263)

## Overview
A recursive optimization function that removes unnecessary capture groups from compiled regular expressions when submatch data is not needed, simplifying the regex structure for better performance.

## Definition
```c
static void removecaptures(struct vars *v, struct subre *t)
```

## Detailed Description
This function implements an important optimization in PostgreSQL's regex engine when the REG_NOSUB flag is set, indicating that the caller doesn't need submatch data. It recursively traverses the subRE tree and removes capture markers from nodes that are not referenced by backreferences. After removing unnecessary captures, it can simplify nodes that no longer need to track match boundaries into simple DFA nodes.

The function operates in multiple phases: first clearing capture numbers and flags for non-backref targets, then recursively processing children and propagating capture flags back up the tree, and finally simplifying nodes that no longer contain captures or backreferences. This optimization can significantly improve regex execution performance by reducing the complexity of the compiled pattern.

## Parameters / Member Variables
- `v`: Pointer to the vars structure containing compilation state
- `t`: Pointer to the subRE node to process for capture removal

## Dependencies
- Functions called/Symbols referenced:
  - [freesubreandsiblings](../f/freesubreandsiblings.md): Called to free child nodes when simplifying
  - [removecaptures](removecaptures.md): Recursive call to process child nodes
  - `BRUSE`: Flag indicating node is referenced by backreferences
  - `CAP`: Flag indicating node contains captures
  - `BACKR`: Flag indicating node contains backreferences
  - `MIXED`: Flag indicating mixed child greediness
  - `[subre](../s/subre.md)`: Sub-regular expression structure type
- Called from (representative examples):
  - `CNOERR`: Main compilation error handling
  - [removecaptures](removecaptures.md): Recursive self-calls for tree traversal

## Notes and Other Information
- Only called when REG_NOSUB flag is set during compilation
- Implements a bottom-up tree traversal to properly propagate capture flags
- Can transform complex capture nodes into simple DFA nodes for optimization
- Preserves backref target nodes to maintain regex semantics
- Part of PostgreSQL's regex compilation optimization pipeline
- The optimization can significantly reduce memory usage and execution time for patterns that don't need submatch data