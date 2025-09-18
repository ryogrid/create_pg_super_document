# set_dump_section

## Location
src/bin/pg_dump/pg_backup_utils.c: 40 - 62

## Overview
Parses a --section command line argument and updates the dump section bitmask to control which parts of a database dump are processed.

## Definition
```c
void set_dump_section(const char *arg, int *dumpSections)
```

## Detailed Description
This function processes command line arguments for the --section option used by pg_dump and pg_restore utilities. It manages a bitmask that determines which sections of a database dump should be processed: pre-data (schema definitions), data (table contents), or post-data (indexes, constraints, triggers). The function supports selective dumping/restoration by allowing users to specify only certain sections.

The function initializes the dumpSections bitmask on first call (when it equals DUMP_UNSECTIONED) and then sets appropriate bits based on the section name provided. If an invalid section name is provided, it logs an error and terminates the program.

## Parameters / Member Variables
- `arg`: String specifying the section name - must be one of "pre-data", "data", or "post-data"
- `dumpSections`: Pointer to integer bitmask that tracks which dump sections are enabled

## Dependencies
- Functions called/Symbols referenced:
  - DUMP_UNSECTIONED (constant: 0xff)
  - DUMP_PRE_DATA (constant: 0x01)
  - DUMP_DATA (constant: 0x02) 
  - DUMP_POST_DATA (constant: 0x04)
  - pg_log_error_hint
  - exit_nicely
- Called from (representative examples):
  - main (pg_dump.c:625)
  - main (pg_restore.c:291)

## Notes and Other Information
- The dumpSections parameter is initialized as DUMP_UNSECTIONED by pg_dump and pg_restore
- First call clears all bits, subsequent calls add specific section bits
- Invalid section names cause program termination with helpful error messages
- Part of the pg_dump/pg_restore utility suite for PostgreSQL database backup and restore