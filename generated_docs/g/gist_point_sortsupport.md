# gist_point_sortsupport

## Location
src/backend/access/gist/gistproc.c: 1745 - 1761

## Overview
Configures sort support for efficient GiST spatial index construction by setting up Z-order-based sorting with optional abbreviation optimization.

## Definition
```c
Datum gist_point_sortsupport(PG_FUNCTION_ARGS)
```

## Detailed Description
This function initializes sort support for GiST spatial index building operations. It configures the sorting framework to use Z-order (Morton order) comparisons for spatial data, which helps maintain spatial locality during index construction. The function supports two modes of operation:

1. **Abbreviated Mode**: When abbreviation is enabled, it sets up an optimized sorting pipeline using abbreviated Z-order values for faster comparisons, with fallback to full comparisons when needed.

2. **Standard Mode**: When abbreviation is disabled, it uses direct Z-order comparisons without abbreviation.

The abbreviation optimization converts bounding boxes to compact Z-order values, enabling faster initial sorting while maintaining the ability to perform full comparisons when abbreviations are insufficient for determining order.

## Parameters / Member Variables
- Uses PostgreSQL's function argument macro system (PG_FUNCTION_ARGS)
- : SortSupport structure obtained from PG_GETARG_POINTER(0) containing sorting configuration

## Dependencies
- Functions called/Symbols referenced:
  - ssup_datum_unsigned_cmp (abbreviated comparison function)
  - gist_bbox_zorder_abbrev_convert (abbreviation converter function)
  - gist_bbox_zorder_abbrev_abort (abbreviation abort callback)
  - gist_bbox_zorder_cmp (full Z-order comparison function)
  - PG_RETURN_VOID (PostgreSQL function return macro)
- Called from (representative examples):
  - Likely registered as a sort support function in PostgreSQL's function catalog
  - Used during GiST index creation and maintenance operations

## Notes and Other Information
- Part of PostgreSQL's fast GiST index building infrastructure
- Supports both abbreviated and non-abbreviated sorting modes
- Uses Z-order encoding to preserve spatial locality in sorted data
- Follows PostgreSQL's function interface conventions with PG_FUNCTION_ARGS
- Critical for efficient spatial index construction performance
- Works in conjunction with the abbreviation functions to optimize sorting of spatial data