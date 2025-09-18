# GetAttributeByName

## Location
src/backend/executor/execUtils.c: 995 - 1057

## Overview
GetAttributeByName extracts a specific attribute value from a HeapTuple by attribute name, providing a convenient interface for C functions that need to access tuple fields dynamically.

## Definition
Datum GetAttributeByName(HeapTupleHeader tuple, const char *attname, bool *isNull)

## Detailed Description
This function provides attribute access by name for HeapTuples, which is commonly needed in user-defined C functions that take composite types as arguments. The function performs a linear search through the tuple's attribute descriptors to find the attribute with the matching name, then uses heap_getattr to extract the actual value.

The function includes comprehensive input validation, checking for NULL parameters and invalid attribute names. It performs a type cache lookup to get the tuple descriptor, which makes it relatively slow for repeated calls. The function constructs a temporary HeapTupleData structure from the HeapTupleHeader because heap_getattr requires the full tuple structure.

When the attribute is not found, the function raises an ERROR rather than returning a default value, ensuring that programming errors are caught immediately rather than causing subtle bugs.

## Parameters / Member Variables
- `tuple`: HeapTupleHeader containing the tuple data to extract from
- `attname`: Name of the attribute to retrieve
- `isNull`: Output parameter set to indicate if the retrieved value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetTypeId (get tuple type OID)
  - HeapTupleHeaderGetTypMod (get tuple type modifier)
  - lookup_rowtype_tupdesc (get tuple descriptor)
  - namestrcmp (compare attribute names)
  - heap_getattr (extract attribute value)
  - ReleaseTupleDesc (release tuple descriptor)
- Called from (representative examples):
  - exec_rt_fetch (runtime tuple access)
  - overpaid (regression test function)
  - c_overpaid (tutorial example function)

## Notes and Other Information
This function is relatively slow due to the type cache lookup and linear search through attributes on each call. For performance-critical code accessing the same tuple type repeatedly, caching the tuple descriptor and using GetAttributeByNum may be more efficient. The function is designed primarily for user-defined functions where convenience is more important than performance.