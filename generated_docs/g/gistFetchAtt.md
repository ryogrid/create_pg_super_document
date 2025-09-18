# gistFetchAtt

## Location
src/backend/access/gist/gistutil.c: 645 - 665

## Overview
A static helper function that initializes a GiST entry with a fetched value by applying the fetch function from the operator class to decompress or reconstruct the original key value.

## Definition
```c
static Datum gistFetchAtt(GISTSTATE *giststate, int nkey, Datum k, Relation r)
```

## Detailed Description
This function performs the reverse operation of compression by applying the fetch function defined in the GiST operator class. It creates a GISTENTRY with the compressed key value, calls the fetch function to decompress or reconstruct the original value, and returns the resulting key. This is used when the original uncompressed value is needed for operations like penalty calculation or consistent checks.

## Parameters / Member Variables
- `giststate`: GiST state information containing operator class functions and collation information
- `nkey`: The index of the key attribute being processed
- `k`: The compressed Datum value that needs to be fetched/decompressed
- `r`: The GiST index relation

## Dependencies
- Functions called/Symbols referenced:
  - gistentryinit
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md)
  - [GISTENTRY](../G/GISTENTRY.md) (struct)
  - [GISTSTATE](../G/GISTSTATE.md) (struct)
- Called from (representative examples):
  - [gistFetchTuple](gistFetchTuple.md)

## Notes and Other Information
- This is a static function, only used within gistutil.c
- The function assumes the fetch function exists in the operator class (fetchFn[nkey])
- It initializes the GISTENTRY as non-leaf (false) since this is typically used for internal node processing
- The fetch operation is the inverse of the compress operation, used to retrieve original values when needed
- Uses the appropriate collation from giststate when calling the fetch function