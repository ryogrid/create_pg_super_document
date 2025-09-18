# dumparcs

## Location
src/backend/regex/regc_nfa.c: 3721 - 3752

## Overview
A debugging function that outputs all outgoing arcs from a given NFA state in human-readable format, organizing them for clear display.

## Definition


## Detailed Description
The  function iterates through and displays all outgoing arcs from a specified NFA state. It traverses the arc chain to find the oldest arc first, then prints each arc in reverse chronological order (oldest to newest) for clearer debugging output. The function formats the output by placing up to 5 arcs per line before adding a newline, making the display more readable for states with many outgoing transitions.

The function uses the arc's outchain and outchainRev pointers to navigate through the linked list of arcs efficiently, ensuring all outgoing transitions are displayed systematically.

## Parameters / Member Variables
- : Pointer to the NFA state whose outgoing arcs should be dumped
- : File stream where the arc information will be written

## Dependencies
- Functions called/Symbols referenced:
  - dumparc (for dumping individual arc information)
  - arc (struct type for representing transitions)
- Called from (representative examples):
  - dumpstate (src/backend/regex/regc_nfa.c:3707)
  - REPLACEARC (src/backend/regex/regcomp.c:238)

## Notes and Other Information
- Only available in debug builds (part of the REG_DEBUG conditional compilation)
- Traverses arc chains starting from the oldest arc for chronological clarity
- Formats output with up to 5 arcs per line for better readability
- Assumes at least one outgoing arc exists (asserts )
- Uses both outchain and outchainRev pointers to navigate the arc linked list
- Part of PostgreSQL's regex engine debugging infrastructure
- Helps visualize state transitions during NFA analysis and optimization
- Essential for understanding the structure and flow of compiled regular expressions