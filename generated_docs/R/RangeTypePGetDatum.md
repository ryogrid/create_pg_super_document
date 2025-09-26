# RangeTypePGetDatum

## Location
src/include/utils/rangetypes.h: 86 - 90

## Overview
A static inline function that converts a RangeType pointer to a Datum value for use in PostgreSQL's function manager interface.

## Definition
static inline Datum
RangeTypePGetDatum(const RangeType *X)

## Detailed Description
RangeTypePGetDatum is a conversion function that wraps a RangeType pointer into a Datum value. This function is the counterpart to DatumGetRangeTypeP and DatumGetRangeTypePCopy, providing the reverse conversion from a RangeType pointer back to a Datum. It uses PostgreSQL's standard PointerGetDatum macro to perform the conversion.

This function is essential for returning range type values from PostgreSQL functions, as all function returns must be in Datum format according to the function manager (fmgr) interface. The function takes a const RangeType pointer, indicating that it does not modify the range type data during the conversion process.

## Parameters / Member Variables
- `X`: A const RangeType pointer to be converted to Datum format

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum (implicit macro for converting pointer to Datum)
- Called from (representative examples):
  - multirange_out (multirange output function)
  - make_range (range construction function)
  - spg_range_quad_choose (SP-GiST index operations)
  - range_gist_fallback_split (GiST index splitting)
  - PG_RETURN_RANGE_P (macro for returning range values)

## Notes and Other Information
- This function is part of the fmgr functions for range type objects
- Provides the reverse conversion of DatumGetRangeTypeP and DatumGetRangeTypePCopy
- Essential for all PostgreSQL functions that return range type values
- The const qualifier on the parameter indicates this is a read-only operation
- Being defined as static inline, it provides zero-overhead abstraction for RangeType-to-Datum conversion
- Widely used across range type implementation, especially in output and index operations
- Critical component of PostgreSQL's type system interface for range types