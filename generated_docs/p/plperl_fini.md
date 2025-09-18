# plperl_fini

## Location
[src/pl/plperl/plperl.c:509-552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L509-L552)

## Overview
A cleanup function that terminates all Perl interpreters and runs their END blocks when the PostgreSQL process exits, while disabling SPI function usage during cleanup to prevent unsafe operations.

## Definition


## Detailed Description
This function serves as the process exit cleanup handler for the PL/Perl extension. It's registered via on_proc_exit() to ensure proper cleanup of Perl interpreters when the PostgreSQL backend process terminates. The function first sets the plperl_ending flag to disable SPI function usage during cleanup (preventing unsafe database operations during termination). If the process is exiting cleanly (code == 0), it systematically destroys all Perl interpreters: first the held interpreter, then all fully-initialized interpreters stored in the hash table. Each interpreter destruction involves activating the interpreter context and calling plperl_destroy_interp, which ensures Perl END blocks are properly executed. The function includes debug logging to track cleanup progress.

## Parameters / Member Variables
- : Exit code (0 for clean exit, non-zero for error exit)
- : Datum argument (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for debug logging)
  - [plperl_destroy_interp](plperl_destroy_interp.md) (to destroy individual interpreters)
  - [hash_seq_init](../h/hash_seq_init.md) (to initialize hash table iteration)
  - [hash_seq_search](../h/hash_seq_search.md) (to iterate through interpreter hash table)
  - [activate_interpreter](../a/activate_interpreter.md) (to set interpreter context before destruction)
  - HASH_SEQ_STATUS (hash table iteration structure)
  - [plperl_interp_desc](plperl_interp_desc.md) (interpreter descriptor structure)
- Called from (representative examples):
  - PostgreSQL process exit handler (registered via on_proc_exit)

## Notes and Other Information
- Registered as an exit handler in select_perl_context() via on_proc_exit(plperl_fini, 0)
- Sets plperl_ending flag to prevent SPI function usage during cleanup for safety
- Only performs cleanup on clean exit (code == 0) to avoid complications during error conditions
- Does not fully undo _PG_init() actions nor make the extension re-initializable
- Ensures Perl END blocks are executed for proper cleanup of Perl-side resources
- Critical for preventing resource leaks and ensuring clean process termination
- Located in src/pl/plperl/plperl.c at lines 509-552
- Debug logging helps track cleanup progress and identify potential issues