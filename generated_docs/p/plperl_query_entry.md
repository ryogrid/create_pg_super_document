# plperl_query_entry

## Location
src/pl/plperl/plperl.c: 199 - 203

## Overview
A hash table entry structure that serves as a key-value pair for storing and retrieving cached query descriptors in PL/Perl.

## Definition


## Detailed Description
The  structure acts as a hash table entry for managing cached query descriptors in PL/Perl. It provides a mapping between query names and their associated query descriptor data, enabling efficient lookup and retrieval of prepared SQL statements. This structure is essential for the query caching mechanism that improves performance by avoiding repeated preparation of the same SQL statements.

## Parameters / Member Variables
- : Character array storing the query identifier name, with size determined by PostgreSQL's standard name length constant
- : Pointer to the associated  structure containing the actual cached query information including the execution plan and parameter details

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN (PostgreSQL constant defining maximum name length)
  - plperl_query_desc (structure containing cached query details)
- Called from (representative examples):
  - select_perl_context
  - plperl_spi_prepare
  - plperl_spi_exec_prepared
  - plperl_spi_query_prepared
  - plperl_spi_freeplan

## Notes and Other Information
- This structure implements the key-value relationship for a hash table used to cache prepared queries
- The use of  ensures compatibility with PostgreSQL's standard naming conventions and limits
- The separation of the hash table entry from the actual query descriptor allows for efficient memory management and lookup operations
- This structure is typically used in conjunction with PostgreSQL's hash table implementation for storing multiple prepared queries per PL/Perl function context