# r_remove_tense_suffixes

## Location
src/backend/snowball/libstemmer/stem_UTF_8_tamil.c: 1479 - 1497

## Overview
Iteratively removes Tamil tense suffixes from words by repeatedly calling the tense suffix removal function until no more changes occur.

## Definition


## Detailed Description
This function serves as a controller for iterative tense suffix removal in Tamil stemming. It implements a loop-based approach that:

1. **Initialization**: Sets the continuation flag  to enable processing
2. **Iterative Processing**: Continuously calls  as long as the flag remains set
3. **Position Management**: Saves and restores cursor position for each iteration
4. **Termination**: Exits the loop when  sets , indicating no more suffixes were found

This iterative approach is necessary because Tamil words can have multiple layered tense suffixes that need to be removed in sequence to reach the root form.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure () containing:
  - Word buffer and position cursors
  - Working variable  used as a continuation flag for the loop

## Dependencies
- Functions called/Symbols referenced:
  - : The core function that identifies and removes individual tense suffixes

- Called from (representative examples):
  - : Main Tamil stemming function

## Notes and Other Information
- Returns 1 on successful completion (always succeeds unless underlying function fails)
- Uses  as a communication mechanism with 
- Implements cursor position saving/restoring pattern typical in Snowball algorithms
- Essential for handling complex Tamil verb morphology where multiple tense markers can be stacked
- The iterative approach ensures complete removal of all tense-related suffixes
- Part of the Tamil verb stemming pipeline in PostgreSQL's full-text search system