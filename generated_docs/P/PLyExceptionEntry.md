# PLyExceptionEntry

## Location
[src/pl/plpython/plpy_spi.h:18-22](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_spi.h#L18-L22)

## Overview
PLyExceptionEntry is a hash table entry structure that maps PostgreSQL SQL state codes to corresponding Python exception objects in the PL/Python language extension.

## Definition


## Detailed Description
PLyExceptionEntry serves as a mapping structure in PL/Python's exception handling system that associates PostgreSQL SQL state codes with their corresponding Python exception objects. This structure is used as entries in a hash table (PLy_spi_exceptions) to enable efficient lookup of Python exceptions based on SQL state codes when PostgreSQL errors occur within PL/Python functions.

The structure is designed to work with PostgreSQL's hash table implementation, where the sqlstate field serves as the hash key. When PostgreSQL encounters an error with a specific SQL state, PL/Python can quickly locate the appropriate Python exception object to raise, providing a seamless bridge between PostgreSQL's error system and Python's exception handling mechanism.

## Parameters / Member Variables
- : An integer representation of the PostgreSQL SQL state code that serves as the hash key for the entry. Must be the first field to work correctly with PostgreSQL's hash table implementation
- : A PyObject pointer to the corresponding Python exception object that should be raised when the associated SQL state occurs

## Dependencies
- Functions called/Symbols referenced:
  - PyObject (Python C API type)
- Called from (representative examples):
  - [PLy_add_exceptions](PLy_add_exceptions.md) (creates hash table using this structure)
  - [PLy_generate_spi_exceptions](PLy_generate_spi_exceptions.md) (populates hash table entries)
  - [PLy_commit](PLy_commit.md) (used for exception handling during transaction commit)
  - [PLy_rollback](PLy_rollback.md) (used for exception handling during transaction rollback)
  - [PLy_spi_subtransaction_abort](PLy_spi_subtransaction_abort.md) (used for exception handling during subtransaction abort)

## Notes and Other Information
- The structure is specifically designed for use with PostgreSQL's hash table implementation, requiring the hash key (sqlstate) to be the first field
- This mapping system allows PL/Python to maintain consistency between PostgreSQL's SQL state-based error reporting and Python's object-oriented exception system
- The hash table using this structure (PLy_spi_exceptions) is created with 256 initial slots and uses HASH_ELEM and HASH_BLOBS flags for efficient SQL state lookups
- The exception objects stored in these entries are created during PL/Python initialization and correspond to various PostgreSQL error conditions as defined in the exception_map array