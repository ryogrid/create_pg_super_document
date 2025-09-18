# WIDGET

## Location
src/test/regress/regress.c: 163 - 167

## Overview
WIDGET is a custom PostgreSQL data type used for regression testing, representing a geometric object with a center point and radius (similar to a circle).

## Definition


## Detailed Description
WIDGET is a test data type defined in the PostgreSQL regression test suite. It was originally called "circle" but was renamed to avoid collision with the built-in circle type (as noted in the comment by Tom Lane from 1997). The structure represents a circular geometric object with a center point and radius, providing a simple custom type for testing PostgreSQL's type system extensibility.

## Parameters / Member Variables
- : Point structure representing the center coordinates (x, y) of the widget
- : Double precision floating-point value representing the radius of the widget

## Dependencies
- Functions called/Symbols referenced:
  - [Point](../P/Point.md) (built-in PostgreSQL geometric type)
  - PG_FUNCTION_INFO_V1 (PostgreSQL function registration macro)
- Called from (representative examples):
  - [widget_in](../w/widget_in.md) (input function for WIDGET type)
  - [widget_out](../w/widget_out.md) (output function for WIDGET type)
  - [pt_in_widget](../p/pt_in_widget.md) (function to test if a point is inside a widget)

## Notes and Other Information
- This is a test-only data type located in src/test/regress/regress.c:163-167
- Includes associated input/output functions (widget_in, widget_out) for type conversion
- Used primarily for testing PostgreSQL's custom data type functionality
- Historical note: renamed from "circle" to avoid naming conflicts with built-in types