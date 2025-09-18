# pg_basetype

## Location
src/backend/utils/adt/misc.c: 583 - 618

## Overview
pg_basetype is a SQL-callable function that returns the base type of a given type OID, unwrapping domain types to reveal their underlying base types.

## Definition


## Detailed Description
This function serves as a SQL-accessible version of the internal  function. It takes a type OID as input and returns the base type OID. If the input type is a domain, the function iteratively traverses the domain hierarchy to find the ultimate base type. If the input type is not a domain, it simply returns the type's own OID.

The function is designed to be robust against race conditions and bogus type OIDs - instead of failing with errors, it returns NULL for invalid or non-existent type OIDs. This makes it safe to use when scanning system catalogs where types might be dropped concurrently.

The implementation uses a loop to handle nested domains (domains built on top of other domains), continuing until it reaches a type that is not a domain.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (Oid): The type OID for which to find the base type

## Dependencies
- Functions called/Symbols referenced:
  -  (to extract the OID argument)
  -  (to look up type information)
  -  (to check if tuple lookup succeeded)
  -  (to extract the Form_pg_type structure)
  -  (to release cache references)
  -  (to return NULL on invalid input)
  -  (to return the result OID)
  -  (PostgreSQL type system structure)
  -  (constant for domain type classification)
- Called from: 
  - No direct callers found in the codebase (SQL-callable function)

## Notes and Other Information
- Located in src/backend/utils/adt/misc.c:583-618
- This function is part of PostgreSQL's SQL API for type introspection
- Unlike the internal getBaseType() function, this version gracefully handles invalid type OIDs by returning NULL
- The function handles nested domains correctly by following the domain chain to its end
- Race condition safety makes it suitable for use in system catalog queries where concurrent DDL operations might occur