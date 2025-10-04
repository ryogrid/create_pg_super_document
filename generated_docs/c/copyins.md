# copyins

## Location
[src/backend/regex/regc_nfa.c:882-970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L882-L970)

## Overview
Copies all incoming arcs from one NFA state to another state, designed specifically for use with brand-new target states that require no duplicate suppression.

## Definition

```c
static void
copyins(struct nfa *nfa,
		struct state *oldState,
		struct state *newState)
```
## Detailed Description
This function copies all incoming arcs from oldState to newState. Unlike moveins(), this function is specifically optimized for the common case where newState is brand-new and has no existing incoming arcs, eliminating the need for duplicate checking.

The function includes the same three-strategy approach as moveins() but with the complex deduplication code paths disabled via #ifdef NOT_USED since they are not needed in current usage:

1. **Active path**: Simple copy without deduplication - iterates through oldState's incoming arcs and creates new arcs to newState using createarc()

2. **Inactive paths (ifdef NOT_USED)**: 
   - Small arc count strategy using cparc() for individual duplicate checking
   - Large arc count strategy using sort-merge approach with sortins_cmp()

The function asserts that newState->nins == 0 to enforce that it should only be called with empty target states.

## Parameters / Member Variables
- `*nfa`: Pointer to the NFA structure containing the states
- `*oldState`: Source state whose incoming arcs will be copied
- `*newState`: Destination state that will receive copies of the arcs (must be empty)
## Dependencies
- Functions called/Symbols referenced:
  - [createarc](createarc.md) (creates new arc)
  - [arc](../a/arc.md) (struct type)
  - BULK_ARC_OP_USE_SORT (macro, ifdef'd out)
  - [cparc](cparc.md) (copy arc function, ifdef'd out)
  - [sortins](../s/sortins.md) (sort function, ifdef'd out)
  - [sortins_cmp](../s/sortins_cmp.md) (comparison function, ifdef'd out)
  - INTERRUPT (cancellation check, ifdef'd out)
  - NISERR (error checking, ifdef'd out)
  - NOTREACHED (assertion, ifdef'd out)
- Called from (representative examples):
  - [pull](../p/pull.md) (src/backend/regex/regc_nfa.c:1749)

## Notes and Other Information
- This is a static function local to the regc_nfa.c file
- Currently optimized for the specific use case of copying to empty states
- The complex deduplication logic is preserved but disabled for potential future use
- Includes assertion to verify newState is empty (newState->nins == 0)
- More efficient than moveins() when deduplication is not needed
- The original arcs in oldState remain unchanged (copy operation, not move)
- The ifdef'd code paths mirror the logic in moveins() but use createarc() instead of changearctarget()

## Simplified Source

```c
static void
copyins(struct nfa *nfa, struct state *oldState, struct state *newState)
{
    assert(oldState != newState);
    assert(newState->nins == 0);  // must be empty target state

    // Simple copy operation - no deduplication needed
    struct arc *a;
    for (a = oldState->ins; a != NULL; a = a->inchain) {
        createarc(nfa, a->type, a->co, a->from, newState);
    }

    // Note: Complex deduplication logic exists but is disabled
    // via #ifdef NOT_USED since current usage only targets empty states
}
```