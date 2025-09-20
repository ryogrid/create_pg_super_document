# specialcolors

## Location
[src/backend/regex/regc_nfa.c:1555-1593](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1555-L1593)

## Overview
Initializes special colors for BOS (Beginning of String), BOL (Beginning of Line), EOS (End of String), and EOL (End of Line) anchors in an NFA (Non-deterministic Finite Automaton) for regular expression processing.

## Definition

```c
struct nfa *nfa)
{
	/* false colors for BOS, BOL, EOS, EOL */
	if (nfa->parent == NULL)
	{
		nfa->bos[0] = pseudocolor(nfa->cm);
		nfa->bos[1] = pseudocolor(nfa->cm);
		nfa->eos[0] = pseudocolor(nfa->cm);
		nfa->eos[1] = pseudocolor(nfa->cm);
	}
	else
	{
		assert(nfa->parent->bos[0] != COLORLESS);
		nfa->bos[0] = nfa->parent->bos[0];
		assert(nfa->parent->bos[1] != COLORLESS);
		nfa->bos[1] = nfa->parent->bos[1];
		assert(nfa->parent->eos[0] != COLORLESS);
		nfa->eos[0] = nfa->parent->eos[0];
		assert(nfa->parent->eos[1] != COLORLESS);
		nfa->eos[1] = nfa->parent->eos[1];
	}
}

/*
 * optimize - optimize an NFA
 *
 * The main goal of this function is not so much "optimization" (though it
 * does try to get rid of useless NFA states) as reducing the NFA to a form
 * the regex executor can handle.  The executor, and indeed the cNFA format
 * that is its input, can only handle PLAIN and LACON arcs.  The output of
 * the regex parser also includes EMPTY (do-nothing) arcs, as well as
 * ^, $, AHEAD, and BEHIND constraint arcs, which we must get rid of here.
 * We first get rid of EMPTY arcs and then deal with the constraint arcs.
 * The hardest part of either job is to get rid of circular loops of the
 * target arc type.  We would have to do that in any case, though, as such a
 * loop would otherwise allow the executor to cycle through the loop endlessly
 * without making any progress in the input string.
 */
static long						/* re_info bits */
optimize(struct nfa *nfa,
		 FILE *f)				/* for debug output;
```
## Detailed Description
This function sets up special pseudo-colors that represent boundary conditions in regular expression matching. The function handles two scenarios:

1. **Root NFA**: If the NFA has no parent (top-level), it creates new pseudo-colors for each boundary type using the  function.
2. **Sub-NFA**: If the NFA is a child of another NFA, it inherits the boundary colors from its parent, ensuring consistency across nested regular expression structures.

The function operates on four boundary types:
-  and : Beginning of string/line colors
-  and : End of string/line colors

These special colors are used internally by the regex engine to handle anchor assertions (^, $, \A, \z) efficiently.

## Parameters / Member Variables
- : Pointer to the NFA structure that needs special colors initialized

## Dependencies
- Functions called/Symbols referenced:
  - : Creates new pseudo-colors for boundary conditions
  - : Constant representing an uninitialized color state
- Called from (representative examples):
  -  (via src/backend/regex/regcomp.c:468)
  -  (via src/backend/regex/regcomp.c:2375)

## Notes and Other Information
- This is a static function, only visible within the regc_nfa.c compilation unit
- The function uses assertions to ensure parent NFA colors are properly initialized before inheritance
- The dual indexing (0 and 1) likely corresponds to different line ending conventions or string vs line boundaries
- This function is part of PostgreSQL's regex engine implementation, which is based on Henry Spencer's regex library