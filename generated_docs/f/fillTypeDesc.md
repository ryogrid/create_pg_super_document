# fillTypeDesc

## Location
src/backend/access/spgist/spgutils.c: 160 - 181

## Overview
fillTypeDesc fills in a SpGistTypeDesc struct with type information retrieved from the PostgreSQL system catalog for a specified data type.

## Definition


## Detailed Description
This function populates a SpGistTypeDesc structure with essential type information by looking up the type in the PostgreSQL system catalog (pg_type). It retrieves fundamental type properties including the type's length, whether it's passed by value, alignment requirements, and storage characteristics. The function performs a system cache lookup to efficiently access the type information and handles error cases where the type is not found.

The function is a utility routine used internally by the SP-GiST access method to gather type metadata needed for proper handling of indexed data types.

## Parameters / Member Variables
- : Pointer to SpGistTypeDesc structure to be filled with type information
- : Object identifier (Oid) of the PostgreSQL data type to look up

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system cache lookup for type information)
  - ObjectIdGetDatum (convert Oid to Datum for cache lookup)
  - HeapTupleIsValid (validate the returned tuple)
  - GETSTRUCT (extract structure from heap tuple)
  - ReleaseSysCache (release system cache entry)
  - elog (error logging)
- Called from (representative examples):
  - spgGetCache (at src/backend/access/spgist/spgutils.c:238, 247, 255, 256)

## Notes and Other Information
- Located in src/backend/access/spgist/spgutils.c:160-181
- This is a static function, only used within the spgutils.c file
- Populates desc fields: type, attlen, attbyval, attalign, attstorage
- Uses system cache (TYPEOID) for efficient type lookup
- Includes error handling for invalid/missing types
- Essential for SP-GiST's type-aware operations and storage management
- The populated SpGistTypeDesc structure is used throughout SP-GiST operations for type-specific handling