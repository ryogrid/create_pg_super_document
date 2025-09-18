# check_amop_signature

## Location
src/backend/access/index/amvalidate.c: 206 - 235

## Overview
Validates the signature (argument and result types) of an operator class operator to ensure it matches expected types exactly.

## Definition
```c
bool check_amop_signature(Oid opno, Oid restype, Oid lefttype, Oid righttype)
```

## Detailed Description
This function verifies that an operator has the correct signature for use in an operator class. It retrieves the operator's catalog information and validates that the result type, operator kind (must be binary 'b'), and both operand types match exactly with the expected values. The function enforces strict type matching since the expected types come from pg_amop catalog entries and should always correspond exactly to the operator's actual signature. Currently hardwired to accept only binary operators.

## Parameters / Member Variables
- `opno`: OID of the operator to validate
- `restype`: Expected result type OID that must match exactly
- `lefttype`: Expected left operand type OID that must match exactly
- `righttype`: Expected right operand type OID that must match exactly

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_operator (operator catalog tuple form)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - GETSTRUCT (macro to extract tuple structure)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache entry release)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to datum conversion)
  - HeapTupleIsValid (tuple validity check)
- Called from (representative examples):
  - [brinvalidate](../b/brinvalidate.md)
  - [ginvalidate](../g/ginvalidate.md)
  - [gistvalidate](../g/gistvalidate.md)
  - [hashvalidate](../h/hashvalidate.md)
  - [btvalidate](../b/btvalidate.md)
  - [spgvalidate](../s/spgvalidate.md)

## Notes and Other Information
- Returns true if signature is valid, false otherwise
- Enforces exact type matching (no coercibility allowed)
- Only accepts binary operators (oprkind = 'b')
- Expected types come from pg_amop entries and should always match exactly
- Part of the access method validation infrastructure
- Simpler than check_amproc_signature due to fixed binary operator requirement
- Located in src/backend/access/index/amvalidate.c:206-235