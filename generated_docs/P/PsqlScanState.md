# PsqlScanState

## Location
[src/include/fe_utils/psqlscan.h:27-31](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/psqlscan.h#L27-L31)

## Overview
PsqlScanState is an abstract type that represents the lexer's internal state for PostgreSQL's SQL scanner in frontend utilities, providing an opaque handle to the complete scanning state.

## Definition
```c
typedef struct PsqlScanStateData *PsqlScanState;
```

## Detailed Description
PsqlScanState is a pointer type that provides an abstract interface to the internal state of PostgreSQL's SQL lexer used in frontend utilities like psql. It serves as an opaque handle to a PsqlScanStateData structure, which contains all the working state needed for SQL parsing operations. This abstraction allows the lexer to maintain multiple independent scanning contexts simultaneously, which is essential for handling nested operations like include files. The design ensures that the lexer can be re-entrant without being recursive, allowing for complex parsing scenarios in PostgreSQL client applications.

The actual implementation details are hidden behind this abstract type, providing a clean interface that separates the public API from the internal implementation details of the scanner.

## Parameters / Member Variables
- This is a typedef to a pointer, so it has no direct member variables
- Points to PsqlScanStateData structure which contains:
  - `scanner`: Flex's state for this PsqlScanState
  - `output_buf`: Current output buffer
  - `buffer_stack`: Stack of variable expansion buffers
  - `scanbufhandle`: Buffer state for outer-level input
  - `encoding`: Current text encoding
  - `safe_encoding`: Whether current encoding is "safe"
  - `std_strings`: Whether string literals are standard
  - `start_state`: Lexer's starting/finishing state
  - `paren_depth`: Depth of nesting in parentheses
  - `xcdepth`: Depth of nesting in slash-star comments
  - `dolqstart`: Current dollar quote start string
  - `begin_depth`: Depth of begin/end pairs for function definitions
  - `callbacks`: Callback functions for variable substitution
  - `cb_passthrough`: Callback passthrough argument

## Dependencies
- Functions called/Symbols referenced:
  - [PsqlScanStateData](PsqlScanStateData.md)
- Called from (representative examples):
  - [process_backslash_command](../p/process_backslash_command.md) (src/bin/pgbench/pgbench.c:5671)
  - [ParseScript](ParseScript.md) (src/bin/pgbench/pgbench.c:5944)
  - [HandleSlashCmds](../H/HandleSlashCmds.md) (src/bin/psql/command.c:221)
  - [MainLoop](../M/MainLoop.md) (src/bin/psql/mainloop.c:35)

## Notes and Other Information
- This type is fundamental to PostgreSQL's frontend SQL parsing infrastructure
- The abstract type design allows for encapsulation and maintainability of the scanner implementation
- Used extensively throughout psql and pgbench for command parsing and SQL statement processing
- The underlying PsqlScanStateData structure maintains state across multiple input lines until explicitly reset
- Supports complex parsing scenarios including variable substitution, nested comments, and function boundary detection