# newarc

## Location
src/backend/regex/regc_nfa.c: 281 - 322

## Overview
Creates a new arc within an NFA (Non-deterministic Finite Automaton) while ensuring no duplicate arcs are created during regex compilation.

## Definition


## Detailed Description
The newarc function is responsible for setting up a new arc within an NFA structure used in PostgreSQL's regex engine. It performs duplicate checking to ensure that no redundant arcs are created, which is important for maintaining NFA efficiency. The function uses an optimization strategy where it checks for duplicates using whichever chain (incoming or outgoing) is shorter to minimize search time. If no duplicate is found, it delegates the actual arc creation to the createarc function.

The function also includes an interrupt check point to allow for operation cancellation during regex compilation, since regex compilation can involve creating many states and arcs.

## Parameters / Member Variables
- : Pointer to the NFA structure that will contain the new arc
- : The type of the arc being created
- : The color associated with the arc (used in regex character classification)
- : Pointer to the source state of the arc
- : Pointer to the destination state of the arc

## Dependencies
- Functions called/Symbols referenced:
  - INTERRUPT (for operation cancellation checks)
  - createarc (for actual arc creation)
- Called from (representative examples):
  - subcolorcvec (color vector processing)
  - newnfa (NFA initialization)
  - cparc (arc copying)
  - makesearch (search pattern creation)
  - cbracket (bracket expression processing)

## Notes and Other Information
- The function includes a comment noting that RAINBOW arcs are theoretically redundant with plain arcs (except for pseudocolors), but this redundancy is not optimized away due to complexity considerations
- The duplicate checking algorithm chooses the shorter chain (from->nouts vs to->nins) for efficiency
- This function is static and only used within the regex NFA compilation module
- The function serves as a key interrupt point for long-running regex compilation operations