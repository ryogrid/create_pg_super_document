# get_opfamily_proc

## Location
src/backend/utils/cache/lsyscache.c: 796 - 826

## Overview
Retrieves the OID of a specific support function for a given operator family and datatypes from the pg_amproc system catalog.

## Definition
```c
Oid get_opfamily_proc(Oid opfamily, Oid lefttype, Oid righttype, int16 procnum)
```

## Detailed Description
This function looks up support functions (procedures) associated with operator families in PostgreSQL's access method framework. It searches the pg_amproc system catalog for a specific combination of operator family, left datatype, right datatype, and procedure number.

Support functions provide the underlying implementation for various operations within an operator family. Different access methods (btree, hash, GiST, GIN, etc.) require different types of support functions. For example:
- Btree requires comparison functions
- Hash requires hash functions  
- GiST requires various geometric support functions

The function returns the OID of the procedure if found, or InvalidOid if no matching entry exists in pg_amproc.

## Parameters / Member Variables
- `opfamily`: The OID of the operator family to search in
- `lefttype`: The OID of the left-hand datatype for the procedure
- `righttype`: The OID of the right-hand datatype for the procedure  
- `procnum`: The procedure number within the operator family (identifies which support function)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache4](../S/SearchSysCache4.md)
  - Form_pg_amproc
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - HeapTupleIsValid
  - Int16GetDatum
  - RegProcedure
- Called from (representative examples):
  - [get_op_hash_functions](get_op_hash_functions.md)
  - [_bt_setup_array_cmp](../b/_bt_setup_array_cmp.md)
  - [_hash_datum2hashkey_type](../h/_hash_datum2hashkey_type.md)
  - [ExecInitExprRec](../E/ExecInitExprRec.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - FinishSortSupportFunction

## Notes and Other Information
- Returns InvalidOid if no matching procedure is found
- Uses the AMPROCNUM system cache for efficient lookups
- Essential for access method operations across PostgreSQL's indexing framework
- The procnum parameter identifies specific support function roles (e.g., HASHSTANDARD_PROC for hash functions)
- Widely used throughout the executor, access methods, and type system
- Located in src/backend/utils/cache/lsyscache.c at lines 796-826