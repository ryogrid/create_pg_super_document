# gistCompressValues

## Location
src/backend/access/gist/gistutil.c: 595 - 644

## Overview
Compresses attribute values for GiST index entries by applying the compress function from the operator class for each key attribute, and includes included attributes for leaf entries.

## Definition


## Detailed Description
This function processes attribute data for GiST index entries by applying compression functions defined by the operator classes. For each key attribute, it creates a GISTENTRY, applies the compress function if one is defined in the operator class, and stores the resulting compressed value. For leaf entries, it also handles included attributes by copying them directly without compression. The function is essential for preparing data before storing it in GiST index pages.

## Parameters / Member Variables
- : GiST state information containing operator class functions and collation information
- : The GiST index relation
- : Array of input attribute values (Datums) to be compressed
- : Array of boolean flags indicating which attributes are NULL
- : Boolean flag indicating whether this is for a leaf page entry
- : Output array where compressed attribute values are stored

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes
  - gistentryinit
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md)
  - [GISTENTRY](../G/GISTENTRY.md) (struct)
  - [GISTSTATE](../G/GISTSTATE.md) (struct)
- Called from (representative examples):
  - [gistSortedBuildCallback](gistSortedBuildCallback.md)
  - gistFormTuple

## Notes and Other Information
- The function handles both key attributes (which get compressed) and included attributes (copied directly for leaf entries)
- Compression is optional - if no compress function is defined in the operator class, the original value is used
- NULL values are handled by storing (Datum) 0 in the output array
- For leaf entries, included attributes are processed after key attributes without compression
- The function uses the collation information from giststate when calling compression functions