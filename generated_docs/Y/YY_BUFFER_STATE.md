# YY_BUFFER_STATE

## Location
src/include/fe_utils/psqlscan_int.h: 56 - 58

## Overview
YY_BUFFER_STATE is a typedef for a pointer to the flex lexer's buffer state structure, used to manage input buffers in PostgreSQL's lexical scanner system.

## Definition


## Detailed Description
YY_BUFFER_STATE is a type definition that represents a pointer to flex's internal buffer state structure. This type is part of PostgreSQL's lexical scanning infrastructure, specifically used in the psql command-line interface and related utilities. The type is defined conditionally to ensure compatibility when the file is included in different contexts - it's primarily intended to be used within flex-generated lexer files where these symbols are already defined, but can also be compiled standalone for header validity checking.

This type is essential for PostgreSQL's multi-lexer architecture, where different parts of the same input can be scanned with physically separate flex lexers. The buffer state allows the system to manage and switch between different input sources during parsing.

## Parameters / Member Variables
This is a typedef to an opaque struct pointer, so the internal structure members are not directly accessible through this type. The actual struct yy_buffer_state is defined by flex and contains flex's internal buffer management data.

## Dependencies
- Functions called/Symbols referenced:
  - (None directly - this is a typedef)
- Called from (representative examples):
  - StackElem (used as buf member)
  - PsqlScanStateData (used as scanbufhandle member)
  - psqlscan_prepare_buffer (returns this type)

## Notes and Other Information
- This typedef is conditionally defined with YY_TYPEDEF_YY_BUFFER_STATE guard to prevent redefinition conflicts
- The actual struct definition comes from flex-generated code
- This is part of PostgreSQL's re-entrant lexer implementation that allows multiple simultaneous lexer operations
- Used in conjunction with variable substitution stack management in psql
- Essential for handling nested include files and variable expansion in PostgreSQL's command-line interface