# ExplainPropertyUInteger

## Location
src/backend/commands/explain.c: 4824 - 4837

## Overview
Explains an unsigned integer-valued property in PostgreSQL EXPLAIN output by formatting a uint64 value and passing it to the generic property explanation function.

## Definition
void ExplainPropertyUInteger(const char *qlabel, const char *unit, uint64 value, ExplainState *es)

## Detailed Description
This function serves as a specialized wrapper around ExplainProperty for handling unsigned integer values. It converts a 64-bit unsigned integer value to its string representation using the UINT64_FORMAT macro, then delegates to ExplainProperty to handle the actual output formatting based on the current explain format (TEXT, XML, JSON, or YAML). This function is particularly useful for statistics that cannot be negative, such as memory sizes, counts, and byte offsets.

## Parameters / Member Variables
- `qlabel`: The label/name of the property to be displayed in the output
- `unit`: Optional unit string to be displayed with the value (e.g., "bytes", "MB")
- `value`: The uint64 unsigned integer value to be explained/displayed
- `es`: Pointer to ExplainState structure containing output format and context information

## Dependencies
- Functions called/Symbols referenced:
  - UINT64_FORMAT (macro for formatting 64-bit unsigned integers)
  - ExplainProperty (generic property explanation function)
- Called from (representative examples):
  - ExplainPrintSerialize (for serialization/deserialization sizes)
  - show_hash_info (for hash bucket counts and memory sizes)
  - show_wal_usage (for WAL byte counts)

## Notes and Other Information
- Less frequently used than ExplainPropertyInteger, but essential for values that must be non-negative
- Uses a 32-byte buffer for formatting, sufficient for any 64-bit unsigned integer representation
- The `true` parameter passed to ExplainProperty indicates this is a numeric property
- Complements ExplainPropertyInteger for complete integer type coverage in explain output
- Primarily used for memory sizes, byte counts, and other inherently non-negative statistics