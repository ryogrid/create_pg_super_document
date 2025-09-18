# GetAttributeByNum

## Location
[src/backend/executor/execUtils.c:1058-1108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L1058-L1108)

## Overview
GetAttributeByNum extracts a specific attribute value from a HeapTuple by attribute number, providing efficient access to tuple fields when the attribute position is known.

## Definition
Datum GetAttributeByNum(HeapTupleHeader tuple, AttrNumber attrno, bool *isNull)

## Detailed Description
This function is the numeric counterpart to GetAttributeByName, providing attribute access by position rather than name. It's more efficient than GetAttributeByName as it avoids the linear search through attribute descriptors, making it suitable for cases where the attribute number is known at compile time or has been previously determined.

Like its name-based counterpart, the function performs comprehensive input validation and constructs a temporary HeapTupleData structure to interface with heap_getattr. It still requires a type cache lookup to get the tuple descriptor, which makes it somewhat expensive for repeated calls on the same tuple type.

The function uses AttributeNumberIsValid to validate the attribute number, ensuring that system columns and invalid attribute numbers are properly rejected. This provides early error detection for programming mistakes.

## Parameters / Member Variables
- `tuple`: HeapTupleHeader containing the tuple data to extract from
- `attrno`: Attribute number (1-based) of the attribute to retrieve
- `isNull`: Output parameter set to indicate if the retrieved value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - AttributeNumberIsValid (validate attribute number)
  - HeapTupleHeaderGetTypeId (get tuple type OID)
  - HeapTupleHeaderGetTypMod (get tuple type modifier)
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md) (get tuple descriptor)
  - [heap_getattr](../h/heap_getattr.md) (extract attribute value)
  - ReleaseTupleDesc (release tuple descriptor)
- Called from (representative examples):
  - exec_rt_fetch (runtime tuple access)

## Notes and Other Information
This function is more efficient than GetAttributeByName since it avoids the linear search for attribute names, but still requires a type cache lookup on each call. For performance-critical code, consider caching the tuple descriptor and using heap_getattr directly. The attribute numbering follows PostgreSQL's convention where user attributes start at 1, and system attributes have negative numbers.