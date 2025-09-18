# regprocin

## Location
[src/backend/utils/adt/regproc.c:66-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L66-L117)

## Overview
Converts a procedure name (or numeric OID) string representation to a RegProcedure OID, serving as the input function for the regproc data type.

## Definition


## Detailed Description
The regprocin function is the input conversion function for PostgreSQL's regproc data type. It takes a C-string representation of a procedure and converts it to the corresponding procedure's OID (Object Identifier). The function supports multiple input formats:

1. **Procedure name**: A simple function name like "myfunction" 
2. **Schema-qualified name**: A fully qualified name like "myschema.myfunction"
3. **Numeric OID**: A direct numeric OID value for symmetry with output routine
4. **Special dash notation**: A "-" character signifies unknown (returns OID 0)

The function performs name resolution using the current search path to find matching pg_proc entries. It ensures that exactly one function matches the given name - throwing errors for both non-existent and ambiguous function references.

## Parameters / Member Variables
- : Input C-string containing the procedure name, schema-qualified name, numeric OID, or "-" for unknown

## Dependencies
- Functions called/Symbols referenced:
  - : Handles "-" notation and numeric OID parsing  
  - : Checks if in bootstrap mode
  - : Parses name into schema-qualified components
  - : Searches pg_proc for matching functions
  - : Error return mechanism with soft error context
  - : Returns the resolved procedure OID
- Called from (representative examples):
  - : Direct function call for regproc conversion

## Notes and Other Information
- Bootstrap mode restriction: In bootstrap processing mode, only numeric OIDs are accepted since all references should be pre-resolved by genbki.pl
- Error handling: Uses soft error context (escontext) to allow caller-controlled error handling rather than immediate ERROR throws
- Ambiguity resolution: Rejects function names that match multiple procedures, requiring explicit schema qualification
- Search path dependency: Resolution follows PostgreSQL's standard search path rules for unqualified names