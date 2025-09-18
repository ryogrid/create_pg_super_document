# newnfa

## Location
[src/backend/regex/regc_nfa.c:47-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L47-L106)

## Overview
Creates and initializes a new NFA (Non-deterministic Finite Automaton) structure for regular expression processing, setting up the required infrastructure including states and arcs.

## Definition


## Detailed Description
The  function allocates and initializes a new NFA structure used in PostgreSQL's regular expression engine. It creates the basic infrastructure needed for pattern matching by setting up initial and final states, along with the necessary arcs and transitions. The function handles memory allocation, creates required states (post, pre, init, final), and establishes basic transitions including start-of-string (^) and end-of-string ($) anchors. If any error occurs during initialization, the function properly cleans up allocated memory using .

## Parameters / Member Variables
- : Pointer to the vars structure containing regex compilation context and error handling
- : Pointer to the colormap structure that manages character class mappings
- : Pointer to parent NFA (NULL if this is the primary NFA), used for nested regex constructs

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