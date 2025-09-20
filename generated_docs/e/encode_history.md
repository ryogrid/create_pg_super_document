# encode_history

## Location
[src/bin/psql/input.c:299-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/input.c#L299-L318)

## Overview
Converts newline characters to a special marker (NL_IN_HISTORY) in all readline history entries to enable safe storage in history files.

## Definition
```c
static void encode_history(void)
```

## Detailed Description
This static function performs an in-place transformation of the entire readline history by replacing newline characters ('\n') with a special marker constant (NL_IN_HISTORY). This encoding is necessary because readline history files use newlines as delimiters between history entries, so embedded newlines within commands must be escaped to prevent corruption of the history file format.

The function iterates through all entries in the current readline history using the BEGIN_ITERATE_HISTORY/END_ITERATE_HISTORY macro pair. For each history entry, it scans through every character in the line and replaces any newline characters with the NL_IN_HISTORY marker. This allows multi-line SQL commands to be safely stored and later restored from the history file.

The operation is performed directly on the history data structure, modifying the original strings in place. The function handles the platform variation where HIST_ENTRY.line may be declared as const char * by casting to char *.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - BEGIN_ITERATE_HISTORY (macro)
  - END_ITERATE_HISTORY (macro)
  - NL_IN_HISTORY (constant)
- Called from (representative examples):
  - [saveHistory](../s/saveHistory.md)

## Notes and Other Information
- This is a static function, only visible within the input.c file
- Operates directly on readline's history data structure in-place
- Must be paired with a corresponding decode operation when reading history back
- Essential for preserving multi-line commands in psql history files
- Handles platform differences in HIST_ENTRY.line declaration (const vs non-const)
- The transformation is reversible - NL_IN_HISTORY markers can be converted back to newlines
- Used as part of the history persistence mechanism in psql