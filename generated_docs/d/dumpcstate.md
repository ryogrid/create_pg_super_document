# dumpcstate

## Location
[src/backend/regex/regc_nfa.c:3860-3890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_nfa.c#L3860-L3890)

## Overview
A debugging function that prints a human-readable representation of a single state within a compiled NFA (Non-deterministic Finite Automaton) structure.

## Definition

```c
static void
dumpcstate(int st,
		   struct cnfa *cnfa,
		   FILE *f)
```
## Detailed Description
The  function outputs detailed debugging information about a specific state in a compiled NFA. It prints the state number followed by a marker indicating whether the state has the CNFA_NOPROGRESS flag (marked with ':') or is a normal state (marked with '.'). Then it iterates through all arcs (transitions) originating from this state, displaying each transition's color/character class and destination state. The function formats output to show up to 5 transitions per line for readability, with special handling for RAINBOW (wildcard) transitions and color vs. constraint distinctions.

The output format includes:
- State number with progress flag indicator (':' for no-progress, '.' for normal)
- Transition arcs showing [color]->destination or [*]->destination for wildcards
- Constraint arcs showing :constraint:->destination for lookahead/lookbehind
- Line wrapping after every 5 transitions for readability

## Parameters / Member Variables
- `st`: The state number to be dumped (0-based index)
- `*cnfa`: Pointer to the compiled NFA structure containing the state
- `*f`: File stream where the debug output will be written
## Dependencies
- Functions called/Symbols referenced:
  - fprintf
  - fflush
  - CNFA_NOPROGRESS
  - COLORLESS
  - RAINBOW
  - struct carc
- Called from (representative examples):
  - [dumpcnfa](dumpcnfa.md) (in regc_nfa.c:3849)
  - REPLACEARC (in regcomp.c:241)

## Notes and Other Information
- This function is only compiled when REG_DEBUG is defined
- Located in src/backend/regex/regc_nfa.c:3860-3890
- Subordinate function of dumpcnfa, used to print individual state details
- Uses a position counter to limit output to 5 transitions per line for better readability
- Distinguishes between regular character classes (colors < ncolors) and constraints (colors >= ncolors)
- RAINBOW represents a wildcard transition that matches any character
- The function handles empty states (states with no outgoing arcs) gracefully
- Part of PostgreSQL's internal regular expression engine debugging infrastructure

## Simplified Source

```c
static void dumpcstate(int st, struct cnfa *cnfa, FILE *f) {
    // Print state number with progress flag indicator
    fprintf(f, "%d%s", st, (cnfa->stflags[st] & CNFA_NOPROGRESS) ? ":" : ".");

    int pos = 1;
    // Print all arcs (transitions) from this state
    for (struct carc *ca = cnfa->states[st]; ca->co != COLORLESS; ca++) {
        // Format different arc types
        if (ca->co == RAINBOW)
            fprintf(f, "\t[*]->%d", ca->to);  // Wildcard
        else if (ca->co < cnfa->ncolors)
            fprintf(f, "\t[%ld]->%d", (long) ca->co, ca->to);  // Color
        else
            fprintf(f, "\t:%ld:->%d", (long) (ca->co - cnfa->ncolors), ca->to);  // Constraint

        // Line wrap after 5 transitions
        if (pos == 5) {
            fprintf(f, "\n");
            pos = 1;
        } else {
            pos++;
        }
    }

    // Final newline if needed
    if (cnfa->states[st]->co == COLORLESS || pos != 1)
        fprintf(f, "\n");
    fflush(f);
}
```