# pg_itm

## Location
src/include/datatype/timestamp.h: 65 - 81

## Overview
The  struct represents a broken-down interval in PostgreSQL, providing a structured way to work with individual time components (microseconds, seconds, minutes, hours, days, months, years) for temporal calculations.

## Definition


## Detailed Description
The  structure is modeled after  used for timestamps but is specifically designed for representing intervals. Unlike timestamps, there's no special interpretation needed for months or years - they're simply zero or non-zero values. This structure allows PostgreSQL to work with interval components in a broken-down format, making it easier to perform arithmetic operations and conversions.

The structure supports negative values in its fields, which is important for representing negative intervals. However, due to the divisions performed during conversion from , only  could potentially reach . This limitation is significant because the code may need to negate values in various code paths.

## Parameters / Member Variables
- : Microseconds component of the interval (0-999999)
- : Seconds component of the interval (0-59)  
- : Minutes component of the interval (0-59)
- : Hours component of the interval (uses int64 for wide range support)
- : Days component of the interval
- : Months component of the interval
- : Years component of the interval

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure)
- Called from (representative examples):
  -  (src/backend/utils/adt/datetime.c:2591)
  -  (src/backend/utils/adt/datetime.c:2675)
  -  (src/backend/utils/adt/datetime.c:4585)
  -  (src/backend/utils/adt/formatting.c:4333)
  -  (src/backend/utils/adt/timestamp.c:986)
  -  (src/backend/utils/adt/timestamp.c:2047)
  -  (src/backend/utils/adt/timestamp.c:2077)
  -  (src/backend/utils/adt/timestamp.c:4254)
  -  (src/backend/utils/adt/timestamp.c:5025)
  -  (src/backend/utils/adt/timestamp.c:5959)

## Notes and Other Information
- Historical design decision: The structure follows the same pattern as  for consistency
- Wide hour field: The  field uses  to support very large hour values that could result from interval calculations
- Negative value support: All fields can be negative, but special care must be taken with  which could reach 
- Used extensively in interval parsing, formatting, and arithmetic operations
- Critical for PostgreSQL's temporal data type system, particularly for INTERVAL data type operations