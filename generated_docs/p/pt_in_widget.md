# pt_in_widget

## Location
[src/test/regress/regress.c:217-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L217-L232)

## Overview
This function tests whether a given point lies within the boundaries of a WIDGET object, which appears to be a circular region defined by a center point and radius.

## Definition


## Detailed Description
pt_in_widget is a PostgreSQL function that implements a geometric containment test for a custom WIDGET data type. The function takes a Point and a WIDGET as arguments and returns a boolean value indicating whether the point falls within the widget's circular boundary. The implementation calculates the Euclidean distance between the input point and the widget's center point, then compares this distance to the widget's radius to determine containment.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that encapsulates:
  - : A Point structure representing the coordinates to test (first argument)
  - : A WIDGET structure containing a center point and radius defining a circular region (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P (extracts Point from function arguments)
  - PG_GETARG_POINTER (extracts WIDGET pointer from function arguments)
  - DirectFunctionCall2 (calls point_distance function)
  - [point_distance](point_distance.md) (calculates Euclidean distance between two points)
  - [DatumGetFloat8](../D/DatumGetFloat8.md) (converts Datum to float8)
  - [PointPGetDatum](../P/PointPGetDatum.md) (converts Point pointer to Datum)
  - PG_RETURN_BOOL (returns boolean result)
- Called from (representative examples):
  - [widget_out](../w/widget_out.md)

## Notes and Other Information
- This function is part of PostgreSQL's regression test suite, demonstrating how to implement custom geometric operations
- The WIDGET type appears to be a test-specific data type representing a circular region
- Uses PostgreSQL's function call interface (PG_FUNCTION_ARGS) for proper integration with the database engine
- Leverages existing point_distance function rather than reimplementing distance calculation
- Located in src/test/regress/regress.c, indicating it's primarily for testing purposes