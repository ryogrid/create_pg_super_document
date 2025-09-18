# check_amoptsproc_signature

## Location
[src/backend/access/index/amvalidate.c:192-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/amvalidate.c#L192-L205)

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
  - [check_amproc_signature](check_amproc_signature.md) (underlying signature validation function)
  - VOIDOID (void type OID constant)
  - INTERNALOID (internal type OID constant)
- Called from (representative examples):
  - [brinvalidate](../b/brinvalidate.md)
  - [ginvalidate](../g/ginvalidate.md)
  - [gistvalidate](../g/gistvalidate.md)
  - [hashvalidate](../h/hashvalidate.md)
  - [btvalidate](../b/btvalidate.md)
  - [spgvalidate](../s/spgvalidate.md)

## Notes and Other Information
- Returns true if signature is valid (void(internal)), false otherwise
- Uses exact matching (no type coercibility allowed)
- Enforces strict signature requirements for options support functions
- Part of the access method validation infrastructure
- Simple wrapper that encapsulates the specific requirements for options functions
- Located in src/backend/access/index/amvalidate.c:192-205