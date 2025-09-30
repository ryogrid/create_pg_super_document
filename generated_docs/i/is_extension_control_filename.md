# is_extension_control_filename

## Location
[src/backend/commands/extension.c:360-367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L360-L367)

## Overview
Utility function that determines whether a given filename represents an extension control file by checking if it ends with the ".control" extension.

## Definition
static bool is_extension_control_filename(const char *filename)

## Detailed Description
This static helper function performs a simple filename validation to identify PostgreSQL extension control files. Extension control files contain metadata about extensions such as default version, comment, dependencies, and other configuration parameters. The function uses string manipulation to locate the file extension and compares it against the expected ".control" suffix.

The function is part of PostgreSQL's extension management infrastructure and serves as a filtering mechanism when scanning directories for extension-related files.

## Parameters / Member Variables
- `filename`: A null-terminated string containing the filename to check for the ".control" extension

## Dependencies
- Functions called/Symbols referenced:
  - strrchr (standard C library function)
  - strcmp (standard C library function)
- Called from (representative examples):
  - [pg_available_extensions](../p/pg_available_extensions.md)
  - [pg_available_extension_versions](../p/pg_available_extension_versions.md)  
  - [extension_file_exists](../e/extension_file_exists.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the src/backend/commands/extension.c file
- The function performs case-sensitive comparison, so ".CONTROL" would not be recognized
- Returns true only if the filename ends exactly with ".control"
- Used primarily during extension discovery operations when scanning extension directories

## Simplified Source

```c
static bool
is_extension_control_filename(const char *filename)
{
    const char *extension;

    // Find the last dot in filename
    extension = strrchr(filename, '.');

    // Check if it ends with ".control"
    return (extension != NULL) && (strcmp(extension, ".control") == 0);
}
```