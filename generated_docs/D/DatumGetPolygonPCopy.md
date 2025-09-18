# DatumGetPolygonPCopy

## Location
src/include/utils/geo_decls.h: 252 - 256

## Overview
DatumGetPolygonPCopy is a static inline function that extracts a POLYGON pointer from a PostgreSQL Datum value and ensures a writable copy is returned, using TOAST decompression with copy semantics for safe modification operations.

## Definition
static inline POLYGON *DatumGetPolygonPCopy(Datum X)

## Detailed Description
This function provides a copy-safe variant of DatumGetPolygonP, specifically designed for scenarios where the retrieved polygon data needs to be modified. It uses PG_DETOAST_DATUM_COPY instead of PG_DETOAST_DATUM, which ensures that a writable copy of the data is always returned, even if the original data could be accessed directly. This prevents accidental modification of shared or read-only polygon data, which could lead to data corruption or unexpected behavior in concurrent operations.

## Parameters / Member Variables
- X: A Datum value containing a reference to POLYGON data that needs to be copied for modification

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_COPY (TOAST decompression with copy semantics)
  - POLYGON (variable-length geometric data type)
- Called from (representative examples):
  - PG_GETARG_POLYGON_P_COPY (macro)

## Notes and Other Information
This function is essential for operations that need to modify polygon data safely without affecting the original stored values. It's defined in src/include/utils/geo_decls.h:252-256. The copy semantics ensure memory safety and data integrity when performing in-place modifications on polygon structures, particularly important in geometric computation functions and data transformation operations.