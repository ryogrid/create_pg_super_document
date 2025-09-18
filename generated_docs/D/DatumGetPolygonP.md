# DatumGetPolygonP

## Location
src/include/utils/geo_decls.h: 247 - 251

## Overview
DatumGetPolygonP is a static inline function that extracts a POLYGON pointer from a PostgreSQL Datum value, with built-in TOAST decompression support for handling variable-length polygon data.

## Definition
static inline POLYGON *DatumGetPolygonP(Datum X)

## Detailed Description
This function serves as a specialized type conversion utility for PostgreSQL's polygon geometric data type. Unlike simpler geometric types, polygons are variable-length structures that may be compressed or stored out-of-line using PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) mechanism. The function uses PG_DETOAST_DATUM to automatically handle decompression and retrieval of potentially toasted polygon data, ensuring that the returned POLYGON pointer references valid, accessible memory.

## Parameters / Member Variables
- X: A Datum value containing a reference to POLYGON data that may be toasted or compressed

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM (TOAST decompression macro)
  - [POLYGON](../P/POLYGON.md) (variable-length geometric data type)
- Called from (representative examples):
  - [gist_poly_compress](../g/gist_poly_compress.md)
  - [spg_box_quad_get_scankey_bbox](../s/spg_box_quad_get_scankey_bbox.md)
  - PG_GETARG_POLYGON_P (macro)

## Notes and Other Information
This function is crucial for handling polygon data in PostgreSQL's geometric subsystem, particularly in indexing operations where polygon data needs to be accessed efficiently. It's defined in src/include/utils/geo_decls.h:247-251. The automatic TOAST handling makes it suitable for working with large polygon datasets that exceed PostgreSQL's inline storage limits.