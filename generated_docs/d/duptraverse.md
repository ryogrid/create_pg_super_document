# duptraverse

## Location
src/backend/regex/regc_nfa.c: 1379 - 1418

## Overview
The recursive heart of the dupnfa function that performs the actual traversal and duplication of NFA states and their outgoing arcs.

## Definition
```c
static void duptraverse(struct nfa *nfa,
                        struct state *s,
                        struct state *stmp) /* s's duplicate, or NULL */
```

## Detailed Description
This function recursively traverses the NFA starting from state s, creating duplicate states and arcs. It uses the tmp pointer in each state to mark already-visited states and to store references to their duplicates. For each state visited, it creates a new duplicate state (if not already created) and then recursively processes all outgoing arcs, duplicating each arc to connect the duplicate states appropriately. The function includes stack overflow protection due to its recursive nature.

## Parameters / Member Variables
- `nfa`: The NFA structure being modified
- `s`: The current state being processed during traversal
- `stmp`: The duplicate of state s, or NULL if a new duplicate should be created

## Dependencies
- Functions called/Symbols referenced:
  - STACK_TOO_DEEP
  - NERR
  - REG_ETOOBIG
  - newstate
  - NISERR
  - duptraverse (recursive call)
  - cparc
- Called from (representative examples):
  - dupnfa
  - duptraverse (recursive calls)

## Notes and Other Information
The function implements stack overflow protection using STACK_TOO_DEEP macro since it's recursive and could potentially exhaust the call stack on deeply nested NFA structures. It carefully manages error states using NISERR checks throughout the traversal. The tmp pointer serves as both a visited marker and a reference to the duplicate state, which is a clever space-efficient design pattern used throughout the NFA duplication process.