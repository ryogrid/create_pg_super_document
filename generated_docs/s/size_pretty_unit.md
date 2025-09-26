# size_pretty_unit

## Location
src/backend/utils/adt/dbsize.c: 38 - 59

## Overview
A structure that defines units of measurement used in PostgreSQL's size formatting functions, specifically for converting byte counts into human-readable formats with appropriate units (bytes, kB, MB, GB, etc.).

## Definition


## Detailed Description
The  structure serves as the foundation for PostgreSQL's byte size formatting system. It defines the characteristics of each unit of measurement used when converting raw byte counts into human-readable strings. The structure is used to create an array of units () that represents the progression from bytes to petabytes, with each unit being a power of 2.

The structure enables flexible size formatting by encapsulating unit properties such as display name, conversion thresholds, rounding behavior, and bit-shift values for efficient calculations. This design supports consistent formatting across PostgreSQL's size-related functions while maintaining precision control for different unit scales.

## Parameters / Member Variables
- : String representation of the unit (e.g., "bytes", "kB", "MB", "GB", "TB", "PB")
- : Upper threshold value used to determine when to switch to the next larger unit, applied before half-rounding calculations
- : Boolean flag indicating whether half-rounding should be applied when converting to this unit (false for bytes, true for larger units)
- : Number of bits to shift left to convert from bytes to this unit (0 for bytes, 10 for kB, 20 for MB, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - Used as array element type in 
- Called from (representative examples):
  -  (formatting functions reference the structure array)
  - 
  - 

## Notes and Other Information
- All units must be powers of 2, as indicated by the comment and enforced by the  field design
- The structure is used to populate a static array  with predefined unit definitions
- When adding new units, documentation and error messages in  must also be updated
- The design allows for efficient bit-shifting operations rather than division for unit conversions
- The  field uses specific values (10 * 1024 for bytes, 20 * 1024 - 1 for others) to control unit selection thresholds