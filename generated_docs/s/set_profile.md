# set_profile

## Location
[src/tools/pg_bsd_indent/args.c:176-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/args.c#L176-L197)

## Overview
The set_profile function reads configuration files (.indent.pro) from the user's home directory and current directory to load indentation settings for the PostgreSQL BSD indent tool.

## Definition


## Detailed Description
This function handles the loading of profile configuration files for the pg_bsd_indent tool. It implements a hierarchical configuration system that:

1. If profile_name is NULL, it loads the default .indent.pro file from the user's home directory
2. If profile_name is provided, it uses the specified profile file (skipping the first 2 characters, typically "--")
3. Always attempts to load .indent.pro from the current working directory
4. Sets the option_source global variable to track where options are being loaded from

The function follows PostgreSQL's configuration precedence where local directory settings can override home directory settings.

## Parameters / Member Variables
- : A string specifying a custom profile file path. If NULL, the default home directory profile is used. When provided, the first 2 characters are skipped (to handle command-line argument format like "--profile-name").

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (standard C library)
  - getenv (standard C library) 
  - fopen (standard C library)
  - [scan_profile](scan_profile.md) (processes the opened profile file)
  - fclose (standard C library)
- Called from (representative examples):
  - [main](../m/main.md) (src/tools/pg_bsd_indent/indent.c:189)

## Notes and Other Information
- Uses MAXPGPATH constant for buffer sizing to ensure safe string operations
- Sets the global variable option_source to track the current source of configuration options
- The function gracefully handles missing profile files by simply skipping them (no error is reported)
- Profile files are processed in order: home directory first, then current directory, allowing local overrides
- Part of the PostgreSQL BSD indent tool infrastructure for code formatting