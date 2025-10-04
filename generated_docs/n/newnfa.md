# newnfa

## Location
[src/backend/regex/regc_nfa.c:47-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L47-L106)

## Overview
Creates and initializes a new NFA (Non-deterministic Finite Automaton) structure for regular expression processing, setting up the required infrastructure including states and arcs.

## Definition

```c
static struct nfa *				/* the NFA, or NULL */
newnfa(struct vars *v,
	   struct colormap *cm,
	   struct nfa *parent)		/* NULL if primary NFA */
```
## Detailed Description
The  function allocates and initializes a new NFA structure used in PostgreSQL's regular expression engine. It creates the basic infrastructure needed for pattern matching by setting up initial and final states, along with the necessary arcs and transitions. The function handles memory allocation, creates required states (post, pre, init, final), and establishes basic transitions including start-of-string (^) and end-of-string ($) anchors. If any error occurs during initialization, the function properly cleans up allocated memory using .

## Parameters / Member Variables
- `*v`: Pointer to the vars structure containing regex compilation context and error handling
- `*cm`: Pointer to the colormap structure that manages character class mappings
- `*parent`: Pointer to parent NFA (NULL if this is the primary NFA), used for nested regex constructs
## Dependencies
- Functions called/Symbols referenced:
  - MALLOC
  - ERR
  - newfstate
  - [newstate](newstate.md)
  - [freenfa](../f/freenfa.md)
  - [rainbow](../r/rainbow.md)
  - [newarc](newarc.md)
  - ISERR
- Called from (representative examples):
  - CNOERR (in regcomp.c)
  - [nfanode](nfanode.md) (in regcomp.c)

## Notes and Other Information
- Returns NULL on allocation failure or initialization error
- Initializes all NFA fields to safe default values before setting up infrastructure
- Creates exactly 4 states: post (@), pre (>), init, and final
- Sets up rainbow transitions and anchor arcs for proper regex matching
- Proper error handling ensures no memory leaks if initialization fails
- The NFA structure is made minimally valid early to ensure safe cleanup via freenfa()

## Simplified Source

```c
static struct nfa *newnfa(struct vars *v, struct colormap *cm, struct nfa *parent) {
    // Allocate new NFA structure
    struct nfa *nfa = (struct nfa *) MALLOC(sizeof(struct nfa));
    if (nfa == NULL) {
        ERR(REG_ESPACE);
        return NULL;
    }

    // Initialize all fields to safe defaults
    nfa->states = NULL;
    nfa->slast = NULL;
    nfa->freestates = NULL;
    nfa->freearcs = NULL;
    nfa->nstates = 0;
    nfa->cm = cm;
    nfa->v = v;
    nfa->parent = parent;

    // Initialize color arrays and flags
    nfa->bos[0] = nfa->bos[1] = COLORLESS;
    nfa->eos[0] = nfa->eos[1] = COLORLESS;
    nfa->flags = 0;
    nfa->minmatchall = nfa->maxmatchall = -1;

    // Create the four required states: post (@), pre (>), init, final
    nfa->post = newfstate(nfa, '@');    // State 0
    nfa->pre = newfstate(nfa, '>');     // State 1
    nfa->init = newstate(nfa);          // Initial state
    nfa->final = newstate(nfa);         // Final state

    // Check for errors in state creation
    if (ISERR()) {
        freenfa(nfa);
        return NULL;
    }

    // Set up basic transitions:
    // Rainbow transition from pre to init (matches any character)
    rainbow(nfa, nfa->cm, PLAIN, COLORLESS, nfa->pre, nfa->init);

    // Start-of-string anchors (^) from pre to init
    newarc(nfa, '^', 1, nfa->pre, nfa->init);
    newarc(nfa, '^', 0, nfa->pre, nfa->init);

    // Rainbow transition from final to post
    rainbow(nfa, nfa->cm, PLAIN, COLORLESS, nfa->final, nfa->post);

    // End-of-string anchors ($) from final to post
    newarc(nfa, '$', 1, nfa->final, nfa->post);
    newarc(nfa, '$', 0, nfa->final, nfa->post);

    // Final error check
    if (ISERR()) {
        freenfa(nfa);
        return NULL;
    }

    return nfa;
}
```