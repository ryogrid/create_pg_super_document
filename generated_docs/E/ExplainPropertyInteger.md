# ExplainPropertyInteger

## Location
[src/backend/commands/explain.c:4811-4823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4811-L4823)

## Overview
Explains an integer-valued property in PostgreSQL EXPLAIN output by formatting an int64 value and passing it to the generic property explanation function.

## Definition

```c
void
ExplainPropertyInteger(const char *qlabel, const char *unit, int64 value,
					   ExplainState *es)
```
## Detailed Description
This function serves as a specialized wrapper around ExplainProperty for handling integer values. It converts a 64-bit signed integer value to its string representation using the INT64_FORMAT macro, then delegates to ExplainProperty to handle the actual output formatting based on the current explain format (TEXT, XML, JSON, or YAML). This ensures consistent integer formatting across all explain output formats while maintaining type safety.

## Parameters / Member Variables
- `qlabel`: The label/name of the property to be displayed in the output
- `unit`: Optional unit string to be displayed with the value (e.g., "ms", "KB")
- `value`: The int64 integer value to be explained/displayed
- `es`: Pointer to ExplainState structure containing output format and context information

## Dependencies
- Functions called/Symbols referenced:
  - INT64_FORMAT (macro for formatting 64-bit integers)
  - [ExplainProperty](ExplainProperty.md) (generic property explanation function)
- Called from (representative examples):
  - [ExplainPrintPlan](ExplainPrintPlan.md) (for plan execution times)
  - [ExplainPrintJIT](ExplainPrintJIT.md) (for JIT compilation statistics)
  - [ExplainNode](ExplainNode.md) (for various node statistics like loops, rows)
  - [show_sort_info](../s/show_sort_info.md) (for sort statistics)
  - [show_hash_info](../s/show_hash_info.md) (for hash table statistics)
  - [show_memoize_info](../s/show_memoize_info.md) (for memoization statistics)
  - [show_buffer_usage](../s/show_buffer_usage.md) (for buffer usage statistics)

## Notes and Other Information
- This function is extensively used throughout the explain system for displaying numeric statistics
- Uses a 32-byte buffer for formatting, which is sufficient for any 64-bit integer representation
- The `true` parameter passed to ExplainProperty indicates this is a numeric property
- Part of a family of type-specific property explanation functions (Integer, UInteger, Float, Bool)