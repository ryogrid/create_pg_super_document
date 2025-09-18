# plperl_proc_ptr

## Location
[src/pl/plperl/plperl.c:161-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L161-L165)

## Overview
The plperl_proc_ptr structure serves as a hash table entry that maps composite keys to cached Perl procedure descriptors, facilitating efficient lookup and management of compiled Perl functions.

## Definition


## Detailed Description
This structure acts as a hash table entry in PostgreSQL's PL/Perl function caching system. It combines a composite key (plperl_proc_key) with a pointer to the actual procedure descriptor (plperl_proc_desc). The design allows the hash table to efficiently map from function identity information (OID, trigger status, user context) to the full compiled procedure data.

The separation between the key structure and the procedure descriptor serves an important purpose: it simplifies error recovery during function compilation. If compile_plperl_function fails, the system can easily clean up without corrupting the hash table structure. The proc_key field must be positioned first to serve as the hash key for PostgreSQL's hash table implementation.

## Parameters / Member Variables
- : Composite hash key containing function OID, trigger flag, and user ID (must be first field for hash table compatibility)
- : Pointer to the plperl_proc_desc structure containing the actual compiled procedure information and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [plperl_proc_key](plperl_proc_key.md) (embedded as hash key)
  - [plperl_proc_desc](plperl_proc_desc.md) (referenced by pointer)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (hash table initialization)
  - [validate_plperl_function](../v/validate_plperl_function.md) (function validation)
  - [compile_plperl_function](../c/compile_plperl_function.md) (function compilation and caching)

## Notes and Other Information
- The proc_key field must be first to satisfy PostgreSQL's hash table key requirements
- Enables O(1) lookup performance for cached procedures based on composite key
- Separates key data from procedure data for better error handling during compilation
- Supports the multi-user, multi-context caching strategy of PL/Perl
- Part of the overall performance optimization strategy that avoids recompiling Perl code
- Hash table entries persist for the lifetime of the backend process
- Critical component in the function lookup path for all PL/Perl function calls