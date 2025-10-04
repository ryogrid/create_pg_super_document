# pull

## Location
[src/backend/regex/regc_nfa.c:1720-1810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L1720-L1810)

## Overview
Pulls a single back constraint arc (^ or BEHIND) backward past its source state, handling the complex logic of constraint propagation through the NFA structure.

## Definition

```c
struct nfa *nfa,
	 struct arc *con,
	 struct state **intermediates)
{
	struct state *from = con->from;
	struct state *to = con->to;
	struct arc *a;
	struct arc *nexta;
	struct state *s;

	assert(from != to);			/* should have gotten rid of this earlier */
	if (from->flag)				/* can't pull back beyond start */
		return 0;
	if (from->nins == 0)
	{							/* unreachable */
		freearc(nfa, con);
		return 1;
	}

	/*
	 * First, clone from state if necessary to avoid other outarcs.  This may
	 * seem wasteful, but it simplifies the logic, and we'll get rid of the
	 * clone state again at the bottom.
	 */
	if (from->nouts > 1)
	{
		s = newstate(nfa);
		if (NISERR())
			return 0;
		copyins(nfa, from, s);	/* duplicate inarcs */
		cparc(nfa, con, s, to); /* move constraint arc */
		freearc(nfa, con);
		if (NISERR())
			return 0;
		from = s;
		con = from->outs;
	}
	assert(from->nouts == 1);

	/* propagate the constraint into the from state's inarcs */
	for (a = from->ins; a != NULL && !NISERR(); a = nexta)
	{
		nexta = a->inchain;
		switch (combine(nfa, con, a))
		{
			case INCOMPATIBLE:	/* destroy the arc */
				freearc(nfa, a);
				break;
			case SATISFIED:		/* no action needed */
				break;
			case COMPATIBLE:	/* swap the two arcs, more or less */
				/* need an intermediate state, but might have one already */
				for (s = *intermediates; s != NULL; s = s->tmp)
				{
					assert(s->nins > 0 && s->nouts > 0);
					if (s->ins->from == a->from && s->outs->to == to)
						break;
				}
				if (s == NULL)
				{
					s = newstate(nfa);
					if (NISERR())
						return 0;
					s->tmp = *intermediates;
					*intermediates = s;
				}
				cparc(nfa, con, a->from, s);
				cparc(nfa, a, s, to);
				freearc(nfa, a);
				break;
			case REPLACEARC:	/* replace arc's color */
				newarc(nfa, a->type, con->co, a->from, to);
				freearc(nfa, a);
				break;
			default:
				assert(NOTREACHED);
				break;
		}
	}

	/* remaining inarcs, if any, incorporate the constraint */
	moveins(nfa, from, to);
	freearc(nfa, con);
	/* from state is now useless, but we leave it to pullback() to clean up */
	return 1;
}

/*
 * pushfwd - push forward constraints forward to eliminate them
 */
static void
pushfwd(struct nfa *nfa,
		FILE *f)				/* for debug output;
```
## Detailed Description
This function implements the core logic for moving constraint arcs backward in the NFA. The process involves several complex steps:

1. **Validation**: Ensures the constraint can be pulled (not from start state, state is reachable)
2. **State Cloning**: If the source state has multiple outgoing arcs, it clones the state to isolate the constraint
3. **Constraint Propagation**: For each incoming arc to the source state, determines how the constraint interacts using 
4. **Action Based on Compatibility**:
   - **INCOMPATIBLE**: Destroys the incompatible arc
   - **SATISFIED**: No action needed (constraint already satisfied)
   - **COMPATIBLE**: Creates intermediate states to maintain both constraint and arc
   - **REPLACEARC**: Replaces the arc's color with the constraint's color

5. **Intermediate State Management**: Reuses existing intermediate states when possible to avoid duplication
6. **Cleanup**: Moves remaining arcs and frees the constraint arc

The function preserves existing states and arcs (except the target constraint) to maintain loop safety in the calling  function.

## Parameters / Member Variables
- : Pointer to the NFA structure being modified
- : The constraint arc to be pulled backward  
- : Pointer to linked list of intermediate states (chained via tmp fields)

## Dependencies
- Functions called/Symbols referenced:
  - : Creates new NFA states
  - : Duplicates incoming arcs to a state
  - : Copies an arc between specified states
  - : Deallocates arc structures
  - : Determines how constraint interacts with other arcs
  - : Moves incoming arcs from one state to another
  - : Creates new arcs
  - : Error checking macro
  - , , , : Constants from combine() results
  - : Assertion constant for impossible cases
- Called from (representative examples):
  -  (src/backend/regex/regc_nfa.c:1662)

## Notes and Other Information
- This is a static function, only visible within the regc_nfa.c compilation unit
- Critical component of constraint elimination in regex optimization
- Designed to be safe for use in loops by not deleting pre-existing states
- Uses intermediate state caching to avoid creating duplicate states for the same predecessor/successor combinations
- The function handles self-loops by asserting  (these should be eliminated earlier)
- Leaves cleanup of useless states to the calling  function
- Part of PostgreSQL's regex engine constraint elimination system

## Simplified Source
```c
static int pull(struct nfa *nfa, struct arc *con, struct state **intermediates) {
    struct state *from = con->from;
    struct state *to = con->to;
    struct arc *a, *nexta;
    struct state *s;

    // Can't pull back beyond start state or if unreachable
    if (from->flag) return 0;
    if (from->nins == 0) {
        freearc(nfa, con);
        return 1;
    }

    // Clone state if it has multiple outgoing arcs
    if (from->nouts > 1) {
        s = newstate(nfa);
        if (NISERR()) return 0;
        copyins(nfa, from, s);
        cparc(nfa, con, s, to);
        freearc(nfa, con);
        from = s;
        con = from->outs;
    }

    // Propagate constraint through incoming arcs
    for (a = from->ins; a != NULL && !NISERR(); a = nexta) {
        nexta = a->inchain;
        switch (combine(nfa, con, a)) {
            case INCOMPATIBLE:
                freearc(nfa, a);
                break;
            case COMPATIBLE:
                // Find or create intermediate state
                for (s = *intermediates; s != NULL; s = s->tmp) {
                    if (s->ins->from == a->from && s->outs->to == to)
                        break;
                }
                if (s == NULL) {
                    s = newstate(nfa);
                    s->tmp = *intermediates;
                    *intermediates = s;
                }
                cparc(nfa, con, a->from, s);
                cparc(nfa, a, s, to);
                freearc(nfa, a);
                break;
            case REPLACEARC:
                newarc(nfa, a->type, con->co, a->from, to);
                freearc(nfa, a);
                break;
        }
    }

    // Move remaining arcs and cleanup
    moveins(nfa, from, to);
    freearc(nfa, con);
    return 1;
}
```