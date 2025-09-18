# toast_datum_size

## Location
[src/backend/access/common/detoast.c:601-646](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/detoast.c#L601-L646)

## Overview
Returns the physical storage size (possibly compressed) of a varlena datum, handling all storage formats including external TOAST references, compressed data, and inline storage.

## Definition


## Detailed Description
The  function calculates and returns the physical storage size of a variable-length datum (varlena) in PostgreSQL. This function is a crucial component of the TOAST (The Oversized-Attribute Storage Technique) system, which manages large data values that exceed the standard page size limitations.

The function handles multiple storage formats for varlena data:

1. **External on-disk storage**: When data is stored externally in TOAST tables, it returns the external size from the TOAST pointer without counting the pointer itself.

2. **External indirect storage**: For indirectly referenced data, it recursively calls itself on the pointed-to datum. The function includes an assertion to prevent nested indirect references.

3. **External expanded storage**: For expanded object headers (used for complex data types like arrays and records), it uses  to determine the flattened size.

4. **Short varlena format**: For small values using the short header format, it uses  to get the size.

5. **Standard inline storage**: For regular inline data (compressed or uncompressed), it uses  to calculate the datum size.

This function is essential for memory management, query planning, and storage optimization in PostgreSQL.

## Parameters / Member Variables
- : A  representing the varlena value whose size needs to be determined. This can be a pointer to various types of varlena structures depending on the storage format.

## Dependencies
- Functions called/Symbols referenced:
  - : Converts Datum to pointer
  - : Macro to check if data is stored externally on disk
  - : Macro to check if data uses indirect external storage
  - : Macro to check if data uses expanded external storage
  - : Macro to check if data uses short varlena format
  - : Macro to extract external storage pointer
  - : Macro to get external storage size
  - : Gets expanded object header pointer
  - : Returns flattened size of expanded object
  - : Gets size of short varlena
  - : Gets size of standard varlena
  - : Converts pointer to Datum
- Called from (representative examples):
  - : SQL function to get column storage size
  - : Macro for calculating indirect pointer sizes
  - Recursive call within itself for indirect references

## Notes and Other Information
- The function handles recursive calls for indirect external references but includes an assertion to prevent infinite recursion from nested indirect datums.
- For external storage, the function returns the size of the actual data, not including the TOAST pointer overhead.
- This function is critical for accurate memory usage calculations and query optimization decisions.
- The function is part of the detoast.c module, which handles the decompression and retrieval of TOASTed data.
- Different storage formats are handled in a specific order, checking for external storage types first before falling back to inline storage calculations.