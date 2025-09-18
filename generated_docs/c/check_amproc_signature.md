# check_amproc_signature

## Location
src/backend/access/index/amvalidate.c: 152 - 191

## Overview
Validates the signature (argument and result types) of an access method support function against expected type constraints.

## Definition
```c
bool check_amproc_signature(Oid funcid, Oid restype, bool exact, int minargs, int maxargs, ...)
```

## Detailed Description
This function verifies that a support function has the correct signature for use in an operator class. It retrieves the function's catalog information and validates that the return type matches exactly, the function is not set-returning, and the argument count falls within the specified range. For argument types, it can perform either exact matching or check for binary coercibility depending on the 'exact' parameter. The function uses variable arguments to specify the expected argument types.

## Parameters / Member Variables
- `funcid`: OID of the function to validate
- `restype`: Expected return type OID that must match exactly
- `exact`: If true, argument types must match exactly; if false, binary coercibility is sufficient
- `minargs`: Minimum number of arguments the function should accept
- `maxargs`: Maximum number of arguments the function should accept
- `...`: Variable arguments specifying the expected argument type OIDs (up to maxargs)

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc (function catalog tuple form)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - GETSTRUCT (macro to extract tuple structure)
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md) (type coercibility check)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache entry release)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to datum conversion)
  - HeapTupleIsValid (tuple validity check)
- Called from (representative examples):
  - [brinvalidate](../b/brinvalidate.md) (multiple calls)
  - [ginvalidate](../g/ginvalidate.md) (multiple calls)
  - [gistvalidate](../g/gistvalidate.md) (multiple calls)
  - [check_amoptsproc_signature](check_amoptsproc_signature.md)
  - [btvalidate](../b/btvalidate.md) (multiple calls)
  - [spgvalidate](../s/spgvalidate.md) (multiple calls)

## Notes and Other Information
- Returns true if signature is valid, false otherwise
- Performs exact return type matching regardless of 'exact' parameter
- Rejects set-returning functions automatically
- Uses variable argument list to handle different numbers of expected arguments
- Critical validation component used across all access method validators
- Located in src/backend/access/index/amvalidate.c:152-191