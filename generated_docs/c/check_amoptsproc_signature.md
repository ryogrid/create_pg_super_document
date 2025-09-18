# check_amoptsproc_signature

## Location
src/backend/access/index/amvalidate.c: 192 - 205

## Overview
Validates the signature of an operator class options support function, which must have the specific signature 'void(internal)'.

## Definition
```c
bool check_amoptsproc_signature(Oid funcid)
```

## Detailed Description
This is a specialized wrapper function that validates operator class options support functions. These functions must have a very specific signature: they must return void, take exactly one argument of type 'internal', and match this signature exactly (no coercibility allowed). The function serves as a convenience wrapper around check_amproc_signature with predefined parameters for options validation.

## Parameters / Member Variables
- `funcid`: OID of the options support function to validate

## Dependencies
- Functions called/Symbols referenced:
  - check_amproc_signature (underlying signature validation function)
  - VOIDOID (void type OID constant)
  - INTERNALOID (internal type OID constant)
- Called from (representative examples):
  - brinvalidate
  - ginvalidate
  - gistvalidate
  - hashvalidate
  - btvalidate
  - spgvalidate

## Notes and Other Information
- Returns true if signature is valid (void(internal)), false otherwise
- Uses exact matching (no type coercibility allowed)
- Enforces strict signature requirements for options support functions
- Part of the access method validation infrastructure
- Simple wrapper that encapsulates the specific requirements for options functions
- Located in src/backend/access/index/amvalidate.c:192-205