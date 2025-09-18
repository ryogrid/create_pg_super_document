# index_getprocid

## Location
src/backend/access/index/indexam.c: 826 - 859

## Overview
Retrieves the procedure OID for a specified support function of an indexed attribute, providing access to index access method support routines.

## Definition
```c
RegProcedure index_getprocid(Relation irel,
                             AttrNumber attnum,
                             uint16 procnum)
```

## Detailed Description
The `index_getprocid` function is a fundamental utility for accessing index access method support procedures. Index access methods require various support routines that are not directly implementations of WHERE-clause query operators but are essential for index operations such as comparison, hashing, distance calculation, and other specialized functions.

These support routines are stored in the `pg_amproc` system catalog and are organized by:
- Access method type (B-tree, GiST, GIN, etc.)
- Operator family and operator class
- Left and right data types for the operations
- Procedure number within the access method's support function set

The function calculates the correct index into the relation's cached support procedure array (`rd_support`) based on the attribute number and procedure number. The indexing formula `(nproc * (attnum - 1)) + (procnum - 1)` ensures that each attribute's support procedures are stored contiguously in the array.

As of PostgreSQL 8.3, support routines are further categorized by left and right data types, but this function specifically returns the "default" procedures where both types equal the operator class's `opcintype`.

## Parameters / Member Variables
- `irel`: Relation structure representing the index relation
- `attnum`: Attribute number (1-based) of the indexed column
- `procnum`: Procedure number (1-based) within the access method's support function set

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro for validation)
  - rd_indam->amsupport (number of support functions for the access method)
  - rd_support (cached array of support procedure OIDs)
- Called from (representative examples):
  - bloom_get_procinfo (BRIN bloom support procedure lookup)
  - inclusion_get_procinfo (BRIN inclusion support procedure lookup)
  - initGinState (GIN index state initialization)
  - initGISTstate (GiST index state initialization)
  - gistbuild (GiST index building)

## Notes and Other Information
- Returns RegProcedure (OID of the procedure) for the requested support function
- Only returns "default" functions where left and right types equal the opclass opcintype
- Non-default functions must be looked up directly from the system cache
- The procedure array is pre-populated during relation cache building for performance
- Essential for index access method implementations to locate their required support functions
- Different index types use different sets of support procedures: B-tree uses comparison functions, GiST uses consistent/union/penalty functions, etc.
- The function includes assertions to validate that procedure numbers are within valid ranges