# initcap_wbnext

## Location
[src/backend/utils/adt/formatting.c:1939-1972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1939-L1972)

## Overview
A simple word boundary iterator function that identifies word boundaries by detecting transitions between alphanumeric and non-alphanumeric characters for use in initial capitalization operations.

## Definition
```c
static size_t initcap_wbnext(void *state)
```

## Detailed Description
The `initcap_wbnext` function implements a basic word boundary detection algorithm used specifically for the `str_initcap` function. It operates by scanning through a UTF-8 encoded string and identifying boundaries where the alphanumeric property of characters changes (from alphanumeric to non-alphanumeric or vice versa).

The function uses Unicode-aware character classification via `pg_u_isalnum` to properly handle multibyte characters. Each call to the function advances through the string and returns the byte offset of the next word boundary. The algorithm maintains state between calls to track the current position and the alphanumeric status of the previous character.

## Parameters / Member Variables
- `state`: Pointer to a `WordBoundaryState` structure containing:
  - `str`: The input string being processed
  - `len`: Total length of the string in bytes
  - `offset`: Current byte offset within the string
  - `init`: Boolean flag indicating if the iterator has been initialized
  - `prev_alnum`: Boolean storing the alphanumeric status of the previous character

## Dependencies
- Functions called/Symbols referenced:
  - [WordBoundaryState](../W/WordBoundaryState.md): State structure for tracking iteration progress
  - [utf8_to_unicode](../u/utf8_to_unicode.md): Convert UTF-8 bytes to Unicode codepoint
  - [pg_u_isalnum](../p/pg_u_isalnum.md): Unicode-aware alphanumeric character test
  - [unicode_utf8len](../u/unicode_utf8len.md): Get byte length of UTF-8 character
- Called from (representative examples):
  - [str_initcap](../s/str_initcap.md): Main initial capitalization function

## Notes and Other Information
- Returns the byte offset of the current word boundary, or the string length when iteration is complete
- The function is designed as a callback for word boundary iteration in `str_initcap`
- Word boundaries are detected at character transitions between alphanumeric and non-alphanumeric characters
- Properly handles UTF-8 multibyte characters by using Unicode-aware functions
- The iterator maintains state between calls, advancing through the string one boundary at a time
- This is a simplified word boundary algorithm compared to full Unicode word boundary rules (UAX #29)

## Simplified Source

```c
static size_t initcap_wbnext(void *state) {
    struct WordBoundaryState *wbstate = (struct WordBoundaryState *) state;

    // Scan through string looking for word boundaries
    while (wbstate->offset < wbstate->len && wbstate->str[wbstate->offset] != '\0') {
        // Get Unicode character at current position
        pg_wchar u = utf8_to_unicode((unsigned char *) wbstate->str + wbstate->offset);
        bool curr_alnum = pg_u_isalnum(u, true);

        // Found word boundary: first character or alphanumeric property changed
        if (!wbstate->init || curr_alnum != wbstate->prev_alnum) {
            size_t prev_offset = wbstate->offset;

            // Update state for next iteration
            wbstate->init = true;
            wbstate->offset += unicode_utf8len(u);
            wbstate->prev_alnum = curr_alnum;

            return prev_offset;  // Return boundary position
        }

        // No boundary found, advance to next character
        wbstate->offset += unicode_utf8len(u);
    }

    // End of string reached
    return wbstate->len;
}
```