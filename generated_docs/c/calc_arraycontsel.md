# calc_arraycontsel

## Location
src/backend/utils/adt/array_selfuncs.c: 337 - 427

## Overview
Core calculation function for array containment selectivity estimation that extracts statistical data and delegates to mcelem_array_selec for the actual probability computation.

## Definition


## Detailed Description
This function serves as the primary computational engine for array containment selectivity estimation. It is responsible for extracting and preparing PostgreSQL's statistical data (most-common-elements and distinct-element-count histograms) before passing the problem to mcelem_array_selec() for the actual selectivity calculation.

The function handles three array containment operators:
-  (contains): estimates selectivity using MCELEM statistics
-  (overlaps): estimates overlap probability using MCELEM statistics  
-  (contained by): uses both MCELEM and DECHIST statistics for more accurate estimation

The function validates the element type compatibility, extracts the constant array value, and adjusts the final result for null fraction in the statistics.

## Parameters
- : Variable statistics data structure containing column statistics
- : Datum representing the constant array value
- : OID of the array element type
- : OID of the containment operator (@>, &&, or <@)

## Dependencies
- Functions called/Symbols referenced:
  - lookup_type_cache
  - DatumGetArrayTypeP
  - statistic_proc_security_check
  - get_attstatsslot
  - mcelem_array_selec
  - free_attstatsslot
  - DEFAULT_SEL
- Called from:
  - arraycontsel (src/backend/utils/adt/array_selfuncs.c:302)

## Notes and Other Information
- Static function (internal to array_selfuncs.c)
- Handles toasted array constants by releasing temporary copies
- Adjusts selectivity for null fraction when statistics are available
- Falls back to default selectivity when element comparison function is unavailable
- Uses both MCELEM (most-common-elements) and DECHIST (distinct-element-count histogram) statistics when available