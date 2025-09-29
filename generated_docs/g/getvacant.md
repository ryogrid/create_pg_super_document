# getvacant

## Location
[src/backend/regex/rege_dfa.c:973-1043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/rege_dfa.c#L973-L1043)

## Overview
A static function that obtains a vacant state set for reuse in the DFA regex engine, clearing its inbound and outbound arcs while preserving the caller's responsibility for clearing internal state.

## Definition

```c
static struct sset *
getvacant(struct vars *v,
		  struct dfa *d,
		  chr *cp,
		  chr *start)
```
## Detailed Description
The  function is a core component of PostgreSQL's regex DFA (Deterministic Finite Automaton) engine that manages state set recycling. It obtains a vacant state set by calling  and then performs comprehensive cleanup of the state set's arc connections. The function meticulously clears both incoming and outgoing arcs to prepare the state set for reuse, while also handling special cases for post-match states and no-progress states by updating the DFA's tracking pointers.

The cleanup process involves two main phases: first, it clears all incoming arcs by traversing the incoming arc chain and removing corresponding outgoing arc references from source states. Second, it removes the state set from the incoming arc chains of all states that it points to via outgoing arcs. This bidirectional cleanup ensures no dangling references remain in the arc structure.

## Parameters / Member Variables
- : Pointer to the vars structure containing regex execution variables and context
- : Pointer to the DFA structure representing the finite automaton
- : Current character pointer in the input string being matched  
- : Pointer to the start of the input string being processed

## Dependencies
- Functions called/Symbols referenced:
  - [pickss](../p/pickss.md) (to obtain a candidate state set)
  - FDEBUG (debugging macro for tracing operations)
  - assert (assertion macro for runtime checks)
- Called from (representative examples):
  - [initialize](../i/initialize.md) (during DFA initialization)
  - [miss](../m/miss.md) (when handling cache misses)
  - LOFF (in regex execution engine)

## Notes and Other Information
The function includes important state tracking for optimization: if the cleared state set was a POSTSTATE (success state), it updates  to remember the furthest successful match position. Similarly, for NOPROGRESS states, it updates . This tracking helps the regex engine make decisions about backtracking and match validation. The function assumes the caller will handle initialization of the state set's internal state representation after the arc cleanup is complete.

## Simplified Source

```c
static struct sset *getvacant(struct vars *v, struct dfa *d, chr *cp, chr *start)
{
    int i;
    struct sset *ss;
    struct sset *p;
    struct arcp ap;
    color co;

    // Get a candidate state set to reuse
    ss = pickss(v, d, cp, start);
    if (ss == NULL)
        return NULL;
    assert(!(ss->flags & LOCKED));

    // Clear out all incoming arcs (including self-referential ones)
    ap = ss->ins;
    while ((p = ap.ss) != NULL) {
        co = ap.co;
        // Remove the outgoing arc from the source state
        p->outs[co] = NULL;
        ap = p->inchain[co];
        p->inchain[co].ss = NULL;  // Clear chain link
    }
    ss->ins.ss = NULL;  // Clear incoming arc list

    // Remove this state from the incoming arc chains of target states
    for (i = 0; i < d->ncolors; i++) {
        p = ss->outs[i];  // Target state for this color
        assert(p != ss);  // No self-references
        if (p == NULL)
            continue;

        // Remove ss from p's incoming arc chain
        if (p->ins.ss == ss && p->ins.co == i) {
            // ss is first in chain - update head
            p->ins = ss->inchain[i];
        } else {
            // ss is somewhere in the chain - find and remove it
            struct arcp lastap = {NULL, 0};

            assert(p->ins.ss != NULL);
            for (ap = p->ins; ap.ss != NULL && !(ap.ss == ss && ap.co == i);
                 ap = ap.ss->inchain[ap.co]) {
                lastap = ap;
            }
            assert(ap.ss != NULL);
            lastap.ss->inchain[lastap.co] = ss->inchain[i];
        }

        // Clear outgoing arc and chain
        ss->outs[i] = NULL;
        ss->inchain[i].ss = NULL;
    }

    // Update tracking for optimization purposes

    // Track furthest success state position
    if ((ss->flags & POSTSTATE) && ss->lastseen != d->lastpost &&
        (d->lastpost == NULL || d->lastpost < ss->lastseen)) {
        d->lastpost = ss->lastseen;
    }

    // Track furthest no-progress state position
    if ((ss->flags & NOPROGRESS) && ss->lastseen != d->lastnopr &&
        (d->lastnopr == NULL || d->lastnopr < ss->lastseen)) {
        d->lastnopr = ss->lastseen;
    }

    return ss;  // Return cleaned state set ready for reuse
}
```