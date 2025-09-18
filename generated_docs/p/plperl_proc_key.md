# plperl_proc_key

## Location
src/pl/plperl/plperl.c: 149 - 159

## Overview
The plperl_proc_key structure serves as a composite hash table key for fast lookup of cached Perl procedure descriptors, combining function identity, trigger status, and user context information.

## Definition


## Detailed Description
This structure defines a composite key used for efficient hash table lookups of plperl_proc_desc entries. The key combines three critical pieces of information that uniquely identify a compiled Perl function context: the function's OID, whether it's a trigger function, and the user context.

The structure enables PostgreSQL to maintain separate compiled versions of the same function when called by different users (for trusted plperl functions) while sharing compilations for untrusted plperlu functions. The separation ensures proper security isolation - trusted functions are compiled separately per user to prevent privilege escalation, while untrusted functions use a shared compilation with user_id set to 0.

The design carefully avoids struct padding by declaring is_trigger as an Oid rather than a bool, ensuring consistent memory layout and hash performance.

## Parameters / Member Variables
- : OID of the PostgreSQL function from pg_proc catalog table, uniquely identifying the function definition
- : Flag indicating whether this is a trigger function (declared as Oid to avoid padding, but functionally boolean)
- : OID of the user calling the function (set to 0 for plperlu functions, actual user OID for plperl functions for security isolation)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - plperl_proc_ptr (used as hash key in procedure pointer structure)
  - _PG_init (during hash table initialization)
  - compile_plperl_function (for function lookup and caching)

## Notes and Other Information
- Designed to avoid struct padding for optimal hash table performance
- Supports multiple cached versions of the same function when called by different users in trusted mode
- User separation enables security isolation for plperl while allowing sharing for plperlu
- Function redeclaration from plperl to plperlu (or vice versa) can result in multiple hash entries, but only one remains valid
- The composite key approach enables O(1) lookup performance for cached procedure descriptors
- Critical for performance as it avoids recompilation of Perl code on repeated function calls