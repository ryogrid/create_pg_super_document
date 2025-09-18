# stemmer_module

## Location
[src/backend/snowball/dict_snowball.c:85-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/dict_snowball.c#L85-L92)

## Overview
A structure that defines the interface for Snowball stemmer modules, containing function pointers and metadata needed to create, operate, and destroy stemmer environments for specific languages and encodings.

## Definition


## Detailed Description
The  structure serves as a registry entry for Snowball stemmer algorithms in PostgreSQL's text search functionality. Each instance represents a specific stemmer for a particular language and encoding combination. The structure encapsulates all necessary information to instantiate, use, and clean up a stemmer environment. It's used in conjunction with the  macro to create static arrays of available stemmers that can be looked up by name and encoding at runtime.

## Parameters / Member Variables
- : Constant string containing the name identifier of the stemmer (e.g., "english", "russian")
- : PostgreSQL encoding type () that this stemmer supports
- : Function pointer to create and initialize a new Snowball environment for this stemmer
- : Function pointer to clean up and destroy a Snowball environment
- : Function pointer to perform the actual stemming operation on words in the environment

## Dependencies
- Functions called/Symbols referenced:
  - [pg_enc](../p/pg_enc.md) (PostgreSQL encoding enumeration)
  - close (function pointer for cleanup operations)
- Called from (representative examples):
  - STEMMER_MODULE (macro that creates stemmer_module instances)
  - [locate_stem_module](../l/locate_stem_module.md) (function that searches for appropriate stemmer modules)

## Notes and Other Information
- This structure is typically populated using the  macro which automatically generates the correct function names based on language and encoding parameters
- The stemmer modules are stored in static arrays and looked up at runtime based on the requested language and database encoding
- All function pointers in the structure correspond to generated Snowball stemmer code for specific language/encoding combinations
- The structure enables PostgreSQL to support multiple stemming algorithms while maintaining a consistent interface