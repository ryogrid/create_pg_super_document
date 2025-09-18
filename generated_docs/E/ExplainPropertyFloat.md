# ExplainPropertyFloat

## Location
[src/backend/commands/explain.c:4838-4851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4838-L4851)

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
  - [psprintf](../p/psprintf.md) (PostgreSQL's sprintf variant that allocates memory)
  - [ExplainProperty](ExplainProperty.md) (generic property explanation function)
  - [pfree](../p/pfree.md) (PostgreSQL's memory deallocation function)
- Called from (representative examples):
  - [ExplainOnePlan](ExplainOnePlan.md) (for planning and execution times)
  - ExplainPrintJIT (for JIT compilation timing statistics)
  - ExplainPrintSerialize (for serialization times)
  - [report_triggers](../r/report_triggers.md) (for trigger execution times)
  - [ExplainNode](ExplainNode.md) (for costs, selectivity, and timing information)
  - [show_instrumentation_count](../s/show_instrumentation_count.md) (for average values)
  - [show_buffer_usage](../s/show_buffer_usage.md) (for hit ratios and timing)
  - [show_modifytable_info](../s/show_modifytable_info.md) (for conflict resolution statistics)

## Notes and Other Information
- Most heavily used of the type-specific property functions due to prevalence of timing and cost metrics
- Uses dynamic memory allocation to handle variable-length formatted strings
- Proper memory management with pfree ensures no memory leaks
- The ndigits parameter allows for consistent precision control across different metric types
- Essential for displaying performance-related statistics that require fractional precision
- The `true` parameter passed to ExplainProperty indicates this is a numeric property