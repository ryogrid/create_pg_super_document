# TParserGet

## Location
[src/backend/tsearch/wparser_def.c:1710-1877](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L1710-L1877)

## Overview
The core parsing engine function that processes text character by character using a state machine to identify and extract tokens from input text for PostgreSQL's full-text search functionality.

## Definition

```c
static bool
TParserGet(TParser *prs)
```
## Detailed Description
TParserGet is the central function in PostgreSQL's word parser that implements a finite state automaton for tokenizing text. It processes the input string one character at a time, transitioning between parser states based on character classifications and predefined rules.

The function operates through these key stages:
1. **Character Processing**: Determines character length using  for multibyte character support
2. **State Machine Execution**: Uses a dispatch table () to find appropriate actions based on current state and character class
3. **Action Execution**: Performs state transitions, stack operations (push/pop), and token recognition
4. **Token Completion**: When A_BINGO flag is set, completes token extraction and returns true

The parser supports:
- Stack-based state management for complex parsing scenarios (nested structures)
- Multiple action flags for different operations (PUSH, POP, CLEAR, MERGE, etc.)
- Character class-based rule matching
- Special handler functions for complex parsing logic
- Comprehensive tracing support for debugging (when WPARSER_TRACE is enabled)

## Parameters / Member Variables
- `*prs`: Pointer to TParser structure containing:
## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (PostgreSQL interrupt handling)
  - [pg_mblen](../p/pg_mblen.md) (multibyte character length calculation)
  - [newTParserPosition](../n/newTParserPosition.md) (stack state creation)
  - [pfree](../p/pfree.md) (memory deallocation)
  - Various state constants (TPS_Base, TPS_Null, etc.)
  - Action flags (A_BINGO, A_POP, A_PUSH, A_CLEAR, A_MERGE, A_CLRALL, A_RERUN)
- Called from:
  - [p_ishost](../p/p_ishost.md) (host validation parsing)
  - [p_isURLPath](../p/p_isURLPath.md) (URL path validation parsing)
  - [prsd_nexttoken](../p/prsd_nexttoken.md) (main token extraction interface)

## Notes and Other Information
- Returns  when a complete token is found (A_BINGO flag set),  when no more tokens available
- Implements sophisticated stack-based parsing for handling nested structures and backtracking
- Includes extensive debugging support via WPARSER_TRACE macro for development and troubleshooting
- Critical for PostgreSQL's text search performance as it processes all searchable text content
- Uses character class dispatch for efficient rule matching rather than character-by-character comparisons

## Simplified Source

```c
static bool TParserGet(TParser *prs) {
    const TParserStateActionItem *item = NULL;

    CHECK_FOR_INTERRUPTS();

    // End of string check
    if (prs->state->posbyte >= prs->lenstr)
        return false;

    prs->token = prs->str + prs->state->posbyte;
    prs->state->pushedAtAction = NULL;

    // Main parsing loop
    while (prs->state->posbyte <= prs->lenstr) {
        // Calculate character length (multibyte support)
        if (prs->state->posbyte == prs->lenstr)
            prs->state->charlen = 0;
        else
            prs->state->charlen = (prs->charmaxlen == 1) ?
                prs->charmaxlen : pg_mblen(prs->str + prs->state->posbyte);

        // Get action item for current state
        if (prs->state->pushedAtAction) {
            item = prs->state->pushedAtAction + 1;  // Resume after POP
            prs->state->pushedAtAction = NULL;
        } else {
            item = Actions[prs->state->state].action;
        }

        // Find matching action by character class
        while (item->isclass) {
            prs->c = item->c;
            if (item->isclass(prs) != 0)
                break;
            item++;
        }

        // Execute special handler if present
        if (item->special)
            item->special(prs);

        // Token found - set up return values
        if (item->flags & A_BINGO) {
            prs->lenbytetoken = prs->state->lenbytetoken;
            prs->lenchartoken = prs->state->lenchartoken;
            prs->state->lenbytetoken = prs->state->lenchartoken = 0;
            prs->type = item->type;
        }

        // Handle stack operations
        if (item->flags & A_POP) {
            // Pop state from stack
            TParserPosition *ptr = prs->state->prev;
            pfree(prs->state);
            prs->state = ptr;
        } else if (item->flags & A_PUSH) {
            // Push current state to stack
            prs->state->pushedAtAction = item;
            prs->state = newTParserPosition(prs->state);
        } else if (item->flags & A_CLEAR) {
            // Clear previous state
            TParserPosition *ptr = prs->state->prev->prev;
            pfree(prs->state->prev);
            prs->state->prev = ptr;
        } else if (item->flags & A_MERGE) {
            // Merge with previous state
            TParserPosition *ptr = prs->state;
            prs->state = prs->state->prev;
            prs->state->posbyte = ptr->posbyte;
            prs->state->poschar = ptr->poschar;
            // ... copy other position fields
            pfree(ptr);
        }

        // Transition to new state if specified
        if (item->tostate != TPS_Null)
            prs->state->state = item->tostate;

        // Check exit conditions
        if ((item->flags & A_BINGO) ||
            (prs->state->posbyte >= prs->lenstr && !(item->flags & A_RERUN)))
            break;

        // Handle rerun or continue after POP
        if (item->flags & (A_RERUN | A_POP))
            continue;

        // Advance position
        if (prs->state->charlen) {
            prs->state->posbyte += prs->state->charlen;
            prs->state->lenbytetoken += prs->state->charlen;
            prs->state->poschar++;
            prs->state->lenchartoken++;
        }
    }

    return (item && (item->flags & A_BINGO));
}
```