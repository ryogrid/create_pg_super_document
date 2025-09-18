# read_extension_script_file

## Location
[src/backend/commands/extension.c:700-740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L700-L740)

## Overview
Reads an SQL script file into a string and converts it to the database encoding for execution during extension installation.

## Definition


## Detailed Description
This function is responsible for reading SQL script files used during PostgreSQL extension installation and preparation. It handles the critical task of encoding conversion, ensuring that script files written in various encodings are properly converted to the database's target encoding before execution.

The function performs several key operations:
1. Reads the entire script file into memory using 
2. Determines the appropriate source encoding from the extension control file or defaults to database encoding
3. Validates that the source string is valid in the expected encoding
4. Converts the encoding to match the database encoding using PostgreSQL's encoding conversion facilities

This ensures that extension scripts can be written in different encodings but will always be executed with proper encoding compatibility.

## Parameters / Member Variables
- : Pointer to ExtensionControlFile containing extension metadata, including encoding information
- : Path to the SQL script file to be read and processed

## Dependencies
- Functions called/Symbols referenced:
  - [read_whole_file](read_whole_file.md) (reads entire file into memory)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (retrieves current database encoding)
  - [pg_verify_mbstr](../p/pg_verify_mbstr.md) (validates multibyte string encoding)
  - [pg_any_to_server](../p/pg_any_to_server.md) (converts encoding to database encoding)
- Called from:
  - [execute_extension_script](../e/execute_extension_script.md)

## Notes and Other Information
- This is a static function, only accessible within the extension.c module
- The function handles encoding conversion transparently, allowing extension scripts to be written in various encodings
- Memory management: The returned string should be freed by the caller
- Error handling is implicit through the called functions (pg_verify_mbstr and pg_any_to_server will raise errors for invalid encodings)
- The function is essential for internationalization support in PostgreSQL extensions