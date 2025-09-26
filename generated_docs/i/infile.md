# infile

## Location
[src/timezone/zic.c:1243-1364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1243-L1364)

## Overview
Parses and processes timezone definition files in the PostgreSQL timezone compilation system, handling various types of timezone data entries including rules, zones, links, and leap seconds.

## Definition
```c
static void infile(const char *name)
```

## Detailed Description
The `infile` function is a comprehensive file parser that processes timezone definition files. It handles multiple types of timezone data formats and performs the following key operations:

1. **File Opening**: Opens the specified file (or stdin if name is "-") and handles file access errors gracefully.

2. **Line-by-Line Processing**: Reads the file line by line, parsing each line into fields and handling continuation lines for multi-line entries.

3. **Field Parsing**: Uses `getfields` to split lines into whitespace-separated fields, converting empty fields ("-") to NULL pointers.

4. **Command Dispatch**: Based on the first field of each line, dispatches to appropriate handler functions:
   - `LC_RULE`: Processes timezone rules via `inrule`
   - `LC_ZONE`: Processes timezone definitions via `inzone`
   - `LC_LINK`: Processes timezone links via `inlink`
   - `LC_LEAP`: Processes leap second definitions via `inleap`
   - `LC_EXPIRES`: Processes expiration dates via `inexpires`

5. **Special Comment Handling**: For leap second files, extracts expiration dates from special comments.

6. **Continuation Handling**: Manages multi-line entries where zone definitions can span multiple lines.

## Parameters / Member Variables
- `name`: Path to the timezone definition file to process, or "-" for standard input

## Dependencies
- Functions called/Symbols referenced:
  - fopen, fgets, strchr (standard C file I/O functions)
  - strerror (standard C error function)
  - [getfields](../g/getfields.md) (field parsing function)
  - [eat](../e/eat.md) (error context setting function)
  - [inzcont](inzcont.md), inrule, inzone, inlink, inleap, inexires (specific line type handlers)
  - [byword](../b/byword.md) (keyword lookup function)
  - [close_file](../c/close_file.md) (file closing utility)
  - EXIT_FAILURE (standard exit code)
- Called from (representative examples):
  - [main](../m/main.md) (in zic.c for timezone file processing)
  - [AlterSystemSetConfigFile](../A/AlterSystemSetConfigFile.md) (in guc.c for configuration file processing)
  - [readfile](../r/readfile.md) (in initdb.c for initialization file processing)
  - Various test frameworks

## Notes and Other Information
- This is a static function with internal linkage in src/timezone/zic.c
- Handles both regular timezone files and leap second files with different parsing rules
- Supports continuation lines for complex timezone definitions that span multiple lines
- Includes robust error handling for file I/O errors, line length limits, and malformed input
- The function maintains state between lines using the `wantcont` flag for continuation processing
- Special handling for PostgreSQL-specific comments in leap second files for expiration tracking
- Uses a lookup table approach to efficiently dispatch different line types to their respective handlers
- Will terminate the program on critical errors like file access failures or malformed input