# free_command

## Location
[src/bin/pgbench/pgbench.c:5614-5633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5614-L5633)

## Overview
Deallocates a Command structure and all its associated dynamically allocated memory to prevent memory leaks.

## Definition

```c
static void
free_command(Command *command)
```
## Detailed Description
The free_command function serves as a destructor for Command structures in pgbench, responsible for properly deallocating all memory associated with a command object. It systematically frees each dynamically allocated component of the Command structure, including the SQL text buffer, parameter arguments, and metadata strings. The function follows a careful cleanup sequence to ensure no memory leaks occur when commands are no longer needed.

The function handles the PQExpBuffer for SQL text using the appropriate termPQExpBuffer function, iterates through all command arguments to free parameter names, and deallocates optional fields like first_line and varprefix. A notable limitation mentioned in the code is that expression trees (expr field) are not recursively freed, as they are currently not needed for the commands that typically get freed (gset commands).

## Parameters / Member Variables
- : Pointer to the Command structure to be deallocated, including all its associated memory

## Dependencies
- Functions called/Symbols referenced:
  - [termPQExpBuffer](../t/termPQExpBuffer.md): Properly deallocates the PQExpBuffer used for SQL text storage
  - [pg_free](../p/pg_free.md): PostgreSQL's memory deallocation function for freeing individual memory blocks
- Called from (representative examples):
  - Script cleanup functions during pgbench termination or error handling

## Notes and Other Information
- The function assumes the Command structure was previously allocated with pg_malloc or similar
- All dynamically allocated string fields (first_line, argv elements, varprefix) are freed individually
- The argc field determines how many argument strings need to be freed from the argv array
- Expression trees in the expr field are intentionally not freed recursively in the current implementation
- This is a static function, used internally within pgbench for memory management
- The function does not perform null pointer checks, assuming valid input from calling code
- Memory deallocation follows the reverse order of allocation to maintain good memory management practices
- Used primarily during cleanup phases when commands are no longer needed or when errors occur during processing