# canonicalize_path_enc

## Location
[src/port/path.c:343-575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L343-L575)

## Overview
The encoding-aware version of path canonicalization that cleans up and normalizes file system paths while properly handling character encoding considerations.

## Definition


## Detailed Description
The  function is the core implementation for path canonicalization in PostgreSQL. It performs comprehensive path normalization while being aware of character encoding to safely handle multi-byte characters. The function modifies the path in-place using a sophisticated state machine approach.

The function performs these operations:
1. **Windows-specific processing**: Converts backslashes to forward slashes and removes trailing quotes
2. **Separator normalization**: Removes trailing slashes and eliminates duplicate adjacent separators
3. **Component processing**: Handles '.' and '..' directory references using a state machine that tracks:
   - Absolute vs relative paths
   - Path depth for proper '..' resolution
   - Parent reference tracking for relative paths

The state machine manages four states:
- : Starting state for absolute paths
- : Absolute path with known directory depth
- : Starting state for relative paths  
- : Relative path containing irreducible parent references
- : Relative path with known directory depth

## Parameters / Member Variables
- : A null-terminated string containing the file system path to be canonicalized. The path is modified in-place.
- : Integer specifying the character encoding of the path string (e.g., PG_UTF8, PG_SQL_ASCII) to ensure safe multi-byte character handling.

## Dependencies
- Functions called/Symbols referenced:
  -  (Windows path conversion)
  - 
  -  (Windows drive handling)
  - 
  - 
  - State constants: , , , , 
- Called from (representative examples):
  -  (src/port/path.c:339)
  -  (src/bin/psql/command.c:1134)
  -  (src/bin/psql/command.c:2788)
  -  (src/bin/psql/command.c:4394)
  -  (src/bin/psql/copy.c:283)

## Notes and Other Information
- This is the encoding-aware variant that should be used when dealing with paths that may contain multi-byte characters
- The function implements a sophisticated state machine to correctly handle complex path combinations like '../dir/..' 
- Windows-specific logic handles drive letters and UNC paths appropriately
- The algorithm ensures that the output path is never longer than the input, making in-place modification safe
- Empty paths are preserved, and paths that reduce to nothing are converted to '.'
- Critical for safe path handling in PostgreSQL's multi-encoding environment, particularly in psql and file operations