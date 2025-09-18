# get_line_style

## Location
[src/fe_utils/print.c:3677-3690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3677-L3690)

## Overview
The get_line_style function returns the appropriate line style format for table printing, providing either a user-specified custom line style or the default ASCII format.

## Definition


## Detailed Description
This utility function implements a simple but important design pattern for PostgreSQL's table printing system. It allows printTableOpt structures to be initialized with all zero values (which is a common C programming convention for default initialization) while still providing a valid line style for table formatting.

The function checks if a custom line style has been specified in the options structure. If opt->line_style is not NULL, it returns that custom style. Otherwise, it returns a pointer to the default ASCII format (pg_asciiformat), which provides standard ASCII characters for table borders, separators, and formatting elements.

This approach maintains backwards compatibility and simplifies initialization code by ensuring that zero-initialized option structures work correctly without requiring explicit setup of every field.

## Parameters / Member Variables
- : Pointer to printTableOpt structure containing table formatting options and potentially a custom line style

## Dependencies
- Functions called/Symbols referenced:
  - pg_asciiformat (default ASCII line style format structure)
  - [printTableOpt](../p/printTableOpt.md) (table options structure type)
- Called from (representative examples):
  - [print_aligned_text](../p/print_aligned_text.md) (for horizontal aligned table output)
  - [print_aligned_vertical](../p/print_aligned_vertical.md) (for vertical aligned table output)  
  - [print_aligned_vertical_line](../p/print_aligned_vertical_line.md) (for specific line formatting in vertical mode)
  - [printPsetInfo](../p/printPsetInfo.md) (for displaying psql settings)
  - [pset_value_string](../p/pset_value_string.md) (for formatting setting values)

## Notes and Other Information
- The function exists primarily to preserve the zero-initialization convention for printTableOpt structures
- This design pattern allows client code to use memset() or static initialization to get default behavior
- The default pg_asciiformat provides standard ASCII table formatting with characters like '+', '-', and '|' for borders and separators
- Custom line styles can include different character sets for borders, such as Unicode box-drawing characters for more sophisticated table appearance
- The function always returns a valid printTextFormat pointer, ensuring that table printing operations never fail due to missing line style information
- The const return type indicates that the returned format structure should not be modified by the caller
- This function is part of the broader table formatting infrastructure that supports multiple output formats and styles in PostgreSQL client utilities