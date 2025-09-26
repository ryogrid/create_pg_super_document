# findconstraintloop

## Location
[src/backend/regex/regc_nfa.c:2469-2557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L2469-L2557)

## Overview
Recursively searches for loops of constraint arcs in the NFA and breaks them when found to prevent infinite loops during regex compilation.

## Definition

```c
struct nfa *nfa, struct state *s)
{
	struct arc *a;

	/* Since this is recursive, it could be driven to stack overflow */
	if (STACK_TOO_DEEP(nfa->v->re))
	{
		NERR(REG_ETOOBIG);
		return 1;				/* to exit as quickly as possible */
	}

	if (s->tmp != NULL)
	{
		/* Already proven uninteresting? */
		if (s->tmp == s)
			return 0;
		/* Found a loop involving s */
		breakconstraintloop(nfa, s);
		/* The tmp fields have been cleaned up by breakconstraintloop */
		return 1;
	}
	for (a = s->outs; a != NULL; a = a->outchain)
	{
		if (isconstraintarc(a))
		{
			struct state *sto = a->to;

			assert(sto != s);
			s->tmp = sto;
			if (findconstraintloop(nfa, sto))
				return 1;
		}
	}

	/*
	 * If we get here, no constraint loop exists leading out from s.  Mark it
	 * with s->tmp == s so we need not rediscover that fact again later.
	 */
	s->tmp = s;
	return 0;
}

/*
 * breakconstraintloop - break a loop of constraint arcs
 *
 * sinitial is any one member state of the loop.  Each loop member's tmp
 * field links to its successor within the loop.  (Note that this function
 * will reset all the tmp fields to NULL.)
 *
 * We can break the loop by, for any one state S1 in the loop, cloning its
 * loop successor state S2 (and possibly following states), and then moving
 * all S1->S2 constraint arcs to point to the cloned S2.  The cloned S2 should
 * copy any non-constraint outarcs of S2.  Constraint outarcs should be
 * dropped if they point back to S1, else they need to be copied as arcs to
 * similarly cloned states S3, S4, etc.  In general, each cloned state copies
 * non-constraint outarcs, drops constraint outarcs that would lead to itself
 * or any earlier cloned state, and sends other constraint outarcs to newly
 * cloned states.  No cloned state will have any inarcs that aren't constraint
 * arcs or do not lead from S1 or earlier-cloned states.  It's okay to drop
 * constraint back-arcs since they would not take us to any state we've not
 * already been in;
```
## Detailed Description
This function implements a depth-first search algorithm to detect constraint loops in the NFA starting from a given state. It uses the temporary field (tmp) of states to track the search path and identify cycles. When a loop is detected, it calls breakconstraintloop() to eliminate the loop and returns 1 to indicate success.

The algorithm works by:
1. **Stack overflow protection**: Checks recursion depth to prevent stack overflow
2. **Cycle detection**: Uses state tmp fields to detect when the search revisits a state in the current path
3. **Loop breaking**: Calls breakconstraintloop() when a cycle is found
4. **Memoization**: Marks states that don't lead to loops with tmp == s to avoid redundant searches

The function employs an optimization where states proven not to be part of any constraint loop are marked with s->tmp == s, allowing subsequent searches to skip them efficiently.

## Parameters / Member Variables
- : Pointer to the NFA structure being analyzed
- : Starting state for the constraint loop search

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP (macro to check recursion depth)
  - NERR (error reporting macro)
  - REG_ETOOBIG (error code for overly complex regex)
  - breakconstraintloop (function to break detected loops)
  - isconstraintarc (checks if an arc is a constraint arc)
  - findconstraintloop (recursive self-call)
- Called from (representative examples):
  - fixconstraintloops (main constraint loop fixing function)
  - findconstraintloop (recursive self-calls)

## Notes and Other Information
- This is a recursive function with potential for deep recursion in complex NFAs
- Uses state tmp fields both for cycle detection and memoization of negative results
- The found loop doesn't necessarily include the starting state - any reachable loop suffices
- Single-state loops are assumed to be already eliminated before this function is called
- Maximum recursion depth is bounded by the longest chain of constraint arcs in the NFA
- Returns 1 if a loop was found and broken, 0 if no loop exists from the starting state
- Tmp fields are guaranteed to be NULL on success return due to breakconstraintloop cleanup