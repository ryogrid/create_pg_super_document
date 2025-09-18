# box_width

## Location
src/backend/utils/adt/geo_ops.c: 808 - 819

## Overview
The  function calculates and returns the width (horizontal magnitude) of a PostgreSQL BOX geometry as a floating-point number.

## Definition


## Detailed Description
This function provides a public interface to calculate the width of a BOX geometry in PostgreSQL. It serves as a wrapper around the internal  function, returning the calculated width as a PostgreSQL FLOAT8 datum that can be used in SQL queries. The width is defined as the horizontal magnitude of the box, calculated as the difference between the high.x and low.x coordinates. This function is part of the arithmetic operators section for boxes.

## Parameters / Member Variables
- : PostgreSQL function call convention containing:
  - First argument (index 0): Pointer to BOX structure for which to calculate width

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts BOX pointer from function arguments
  - : Internal function that calculates the width of a BOX (high.x - low.x)
  - : Returns floating-point result to PostgreSQL
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function system)

## Notes and Other Information
- This function is typically invoked through PostgreSQL's SQL function call system (e.g., SELECT box_width(box_column))
- The width is calculated as the difference between the rightmost (high.x) and leftmost (low.x) x-coordinates
- The internal  function uses  (floating-point subtraction) to compute high.x - low.x
- Uses floating-point arithmetic (FLOAT8), so standard floating-point precision considerations apply
- Part of the "Arithmetic" operators section for BOX geometries
- Located in src/backend/utils/adt/geo_ops.c:808-819