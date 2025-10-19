# decode_history

## Location
[src/bin/psql/input.c:319-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/input.c#L319-L343)

## Overview
Reverses the encoding of newline characters in readline history entries by converting the special NL_IN_HISTORY character back to actual newline characters.

## Definition

```c
static void
decode_history(void)
```
## Detailed Description
The  function is responsible for reversing the newline encoding that PostgreSQL's psql applies to history entries. When multi-line SQL commands are stored in readline history, actual newline characters ('\n') are encoded as NL_IN_HISTORY (0x01) to preserve the command structure while avoiding issues with readline's handling of embedded newlines.

This function iterates through all history entries using the BEGIN_ITERATE_HISTORY/END_ITERATE_HISTORY macros and converts any NL_IN_HISTORY characters back to '\n' characters. This decoding process is typically performed when initializing the input system to ensure that previously saved multi-line commands are properly restored with their original formatting.

The function handles the complexity of different readline implementations (libreadline vs libedit) through the iterator macros, which automatically detect the correct traversal direction for the history list.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - BEGIN_ITERATE_HISTORY (macro for iterating history entries)
  - END_ITERATE_HISTORY (macro for ending history iteration)
  - NL_IN_HISTORY (constant: 0x01, used as newline placeholder)

- Called from (representative examples):
  - [initializeInput](../i/initializeInput.md) (in src/bin/psql/input.c:394)

## Notes and Other Information
- This function assumes that NL_IN_HISTORY (0x01) will never be entered by the user or appear inside multi-byte strings
- The function modifies history entries in-place by directly manipulating the line content
- Some platforms declare HIST_ENTRY.line as const char *, requiring a cast to char * for modification
- The decoding is the reverse operation of encode_history, which converts '\n' to NL_IN_HISTORY when saving history
- This mechanism is necessary because readline routines cannot properly handle 0x00 characters, making 0x01 a safer choice for the placeholder

## Simplified Source

```c
static void decode_history(void) {
    // Iterate through all history entries
    BEGIN_ITERATE_HISTORY(cur_hist);
    {
        char *cur_ptr;

        // Convert special markers back to newlines
        for (cur_ptr = (char *) cur_hist->line; *cur_ptr; cur_ptr++) {
            if (*cur_ptr == NL_IN_HISTORY) {
                *cur_ptr = '\n';
            }
        }
    }
    END_ITERATE_HISTORY();
}
```