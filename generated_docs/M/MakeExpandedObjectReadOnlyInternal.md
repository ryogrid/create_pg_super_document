# MakeExpandedObjectReadOnlyInternal

## Location
src/backend/utils/adt/expandeddatum.c: 95 - 117

## Overview
Converts a read-write expanded object Datum to its read-only equivalent, or returns the original Datum unchanged if it's not a read-write expanded object.

## Definition

```c
Datum
MakeExpandedObjectReadOnlyInternal(Datum d)
```
## Detailed Description
MakeExpandedObjectReadOnlyInternal is responsible for converting read-write expanded object references to read-only references. This conversion is important for maintaining data integrity when passing expanded objects to contexts where they should not be modified.

The function first checks if the input Datum represents a read-write expanded object using VARATT_IS_EXTERNAL_EXPANDED_RW. If it's not a read-write expanded object, the function returns the original Datum unchanged. If it is a read-write expanded object, the function extracts the ExpandedObjectHeader pointer and returns the read-only Datum reference (eoh_ro_ptr) instead of the read-write reference.

This function is typically called indirectly through the MakeExpandedObjectReadOnly macro, which includes additional null checks and type validation.

## Parameters / Member Variables
- : The Datum that may contain a read-write expanded object reference to be converted

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_EXPANDED_RW (macro)
  - DatumGetPointer (macro)
  - DatumGetEOHP
  - EOHPGetRODatum (macro)
- Types referenced:
  - Datum
  - ExpandedObjectHeader
- Called from (representative examples):
  - ExecInterpExpr (multiple locations)
  - FunctionReturningBool
  - MakeExpandedObjectReadOnly (macro)

## Notes and Other Information
- The caller must ensure that the datum is a non-null varlena value
- The function performs no action if the input is not a read-write expanded object
- This is part of PostgreSQL's expanded object system that allows efficient in-memory representations
- The conversion from read-write to read-only helps prevent accidental modifications in contexts where the data should be immutable
- Used extensively in the expression evaluation system to ensure proper access control
- The function is defined in src/backend/utils/adt/expandeddatum.c:95-117