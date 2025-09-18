# ExplainPropertyFloat

## Location
src/backend/commands/explain.c: 4838 - 4851

## Overview
Explains a floating-point-valued property in PostgreSQL EXPLAIN output by formatting a double value with specified precision and passing it to the generic property explanation function.

## Definition
void ExplainPropertyFloat(const char *qlabel, const char *unit, double value, int ndigits, ExplainState *es)

## Detailed Description
This function serves as a specialized wrapper around ExplainProperty for handling floating-point values. It dynamically allocates memory to format a double-precision floating-point number with a specified number of fractional digits using psprintf, then delegates to ExplainProperty to handle the actual output formatting. The function ensures proper memory management by freeing the allocated buffer after use. This is particularly important for displaying performance metrics like execution times, costs, and ratios that require decimal precision.

## Parameters / Member Variables
- `qlabel`: The label/name of the property to be displayed in the output
- `unit`: Optional unit string to be displayed with the value (e.g., "ms", "cost units")
- `value`: The double-precision floating-point value to be explained/displayed
- `ndigits`: Number of fractional digits to display after the decimal point
- `es`: Pointer to ExplainState structure containing output format and context information

## Dependencies
- Functions called/Symbols referenced:
  - psprintf (PostgreSQL's sprintf variant that allocates memory)
  - ExplainProperty (generic property explanation function)
  - pfree (PostgreSQL's memory deallocation function)
- Called from (representative examples):
  - ExplainOnePlan (for planning and execution times)
  - ExplainPrintJIT (for JIT compilation timing statistics)
  - ExplainPrintSerialize (for serialization times)
  - report_triggers (for trigger execution times)
  - ExplainNode (for costs, selectivity, and timing information)
  - show_instrumentation_count (for average values)
  - show_buffer_usage (for hit ratios and timing)
  - show_modifytable_info (for conflict resolution statistics)

## Notes and Other Information
- Most heavily used of the type-specific property functions due to prevalence of timing and cost metrics
- Uses dynamic memory allocation to handle variable-length formatted strings
- Proper memory management with pfree ensures no memory leaks
- The ndigits parameter allows for consistent precision control across different metric types
- Essential for displaying performance-related statistics that require fractional precision
- The `true` parameter passed to ExplainProperty indicates this is a numeric property