# r_remove_tense_suffixes

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_tamil.c:1479-1497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_tamil.c#L1479-L1497)

## Overview
Iteratively removes Tamil tense suffixes from words by repeatedly calling the tense suffix removal function until no more changes occur.

## Definition

```c
}

static int r_remove_tense_suffixes(struct SN_env * z)
```
## Detailed Description
This function serves as a controller for iterative tense suffix removal in Tamil stemming. It implements a loop-based approach that:

1. **Initialization**: Sets the continuation flag  to enable processing
2. **Iterative Processing**: Continuously calls  as long as the flag remains set
3. **Position Management**: Saves and restores cursor position for each iteration
4. **Termination**: Exits the loop when  sets , indicating no more suffixes were found

This iterative approach is necessary because Tamil words can have multiple layered tense suffixes that need to be removed in sequence to reach the root form.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure () containing:
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

## Simplified Source

```c
static int r_remove_tense_suffixes(struct SN_env * z) {
    // Initialize flag to enable processing
    z->I[1] = 1;

    // Continue removing tense suffixes until none remain
    while (z->I[1]) {
        // Save current cursor position
        int saved_position = z->c;

        // Attempt to remove a tense suffix
        // r_remove_tense_suffix will set z->I[1] = 0 if no suffix found
        int saved_position_for_function = z->c;
        r_remove_tense_suffix(z);
        z->c = saved_position_for_function; // Restore position after function call

        // If no suffix was removed (z->I[1] = 0), exit loop
        // If suffix was removed (z->I[1] = 1), continue loop
    }

    return 1; // Always successful
}
```