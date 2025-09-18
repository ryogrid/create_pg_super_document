# gtsquery_union

## Location
src/backend/utils/adt/tsquery_gist.c: 89 - 106

## Overview
gtsquery_union is a GiST union function that combines multiple TSQuerySign entries into a single unified signature by performing bitwise OR operations.

## Definition


## Detailed Description
This function is part of the GiST index support for TSQuery data types. It implements the union operation required by GiST for combining multiple index entries into a parent node entry. The function takes a vector of TSQuerySign entries and creates a unified signature that represents the union of all contained signatures.

The union process works by:
1. Initializing the result signature to 0
2. Iterating through all entries in the entry vector
3. Performing bitwise OR operations to combine each signature
4. Returning the combined signature that encompasses all input signatures

This ensures that searches will correctly traverse the index tree, as the parent signature contains all bits set in any of its children.

## Parameters / Member Variables
- : Pointer to GistEntryVector containing all TSQuerySign entries to be combined
- : Pointer to integer where the size of the result should be stored

## Dependencies
- Functions called/Symbols referenced:
  - [GistEntryVector](../G/GistEntryVector.md) (struct type for entry collection)
  - TSQuerySign (signature type)
  - GETENTRY (macro to extract entry from vector)
  - PG_RETURN_TSQUERYSIGN (return macro for TSQuerySign)
- Called from (representative examples):
  - GiST index operations (no direct references found in codebase)

## Notes and Other Information
- This is a PostgreSQL extension function following PG_FUNCTION_ARGS convention
- Uses bitwise OR to combine signatures, ensuring no information is lost
- Sets *size to sizeof(TSQuerySign) to indicate the size of the returned value
- Essential for proper GiST index tree construction and maintenance
- The resulting union signature may have more bits set than any individual input, which is expected for proper index operation
- Part of the TSQuery GiST operator class implementation for efficient text search indexing