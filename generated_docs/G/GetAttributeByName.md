# GetAttributeByName

## Location
[src/backend/executor/execUtils.c:995-1057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L995-L1057)

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
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md) (get tuple descriptor)
  - [namestrcmp](../n/namestrcmp.md) (compare attribute names)
  - [heap_getattr](../h/heap_getattr.md) (extract attribute value)
  - ReleaseTupleDesc (release tuple descriptor)
- Called from (representative examples):
  - [exec_rt_fetch](../e/exec_rt_fetch.md) (runtime tuple access)
  - [overpaid](../o/overpaid.md) (regression test function)
  - [c_overpaid](../c/c_overpaid.md) (tutorial example function)

## Notes and Other Information
This function is relatively slow due to the type cache lookup and linear search through attributes on each call. For performance-critical code accessing the same tuple type repeatedly, caching the tuple descriptor and using GetAttributeByNum may be more efficient. The function is designed primarily for user-defined functions where convenience is more important than performance.

## Simplified Source

```c
// Simplified version of GetAttributeByName
Datum GetAttributeByName(HeapTupleHeader tuple, const char *attname, bool *isNull) {
    AttrNumber attrno;
    Datum result;
    TupleDesc tupDesc;
    HeapTupleData tmptup;
    int i;

    // Input validation: check for NULL parameters
    if (attname == NULL)
        elog(ERROR, "invalid attribute name");
    if (isNull == NULL)
        elog(ERROR, "a NULL isNull pointer was passed");
    if (tuple == NULL) {
        *isNull = true;
        return (Datum) 0;
    }

    // Get tuple descriptor from tuple type information
    Oid tupType = HeapTupleHeaderGetTypeId(tuple);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(tuple);
    tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    // Search for attribute by name (linear search through all attributes)
    attrno = InvalidAttrNumber;
    for (i = 0; i < tupDesc->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(tupDesc, i);
        if (namestrcmp(&(att->attname), attname) == 0) {
            attrno = att->attnum;
            break;
        }
    }

    // Error if attribute not found
    if (attrno == InvalidAttrNumber)
        elog(ERROR, "attribute \"%s\" does not exist", attname);

    // Create temporary HeapTuple structure for heap_getattr
    tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
    ItemPointerSetInvalid(&(tmptup.t_self));
    tmptup.t_tableOid = InvalidOid;
    tmptup.t_data = tuple;

    // Extract the attribute value
    result = heap_getattr(&tmptup, attrno, tupDesc, isNull);

    // Clean up tuple descriptor
    ReleaseTupleDesc(tupDesc);

    return result;
}
```

Key simplifications made:
- Consolidated variable declarations for clarity
- Added inline comments explaining each major step
- Simplified the tuple type extraction by declaring variables inline
- Focused on the main execution path while preserving all essential logic
- Maintained all error handling as it's critical for function correctness