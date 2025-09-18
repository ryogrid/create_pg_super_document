# load_resultmap

## Location
[src/test/regress/pg_regress.c:615-688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L615-L688)

## Overview
Scans the resultmap file to determine which platform-specific expected files to use during regression testing.

## Definition


## Detailed Description
The  function reads a "resultmap" file from the input directory to identify platform-specific expected output files for PostgreSQL regression tests. The resultmap file format uses entries like , where the hostplatformpattern is evaluated as a regular expression against the current platform's config.guess output. When a pattern matches the current host platform, the corresponding test name, file type, and expected result file are stored in a linked list for later use during test execution.

The function implements a last-match-wins strategy by prepending new entries to the front of the resultmap list, ensuring that later entries in the file take precedence over earlier ones in cases of ambiguous matches.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - fopen
  - bail
  - [string_matches_pattern](../s/string_matches_pattern.md)
  - pg_malloc
  - _resultmap (struct type)
- Called from (representative examples):
  - [initialize_environment](../i/initialize_environment.md)

## Notes and Other Information
- The resultmap file is optional - the function silently returns if the file doesn't exist
- Uses a simplified regular expression matching via  rather than full regex support
- Builds a linked list of platform-specific result mappings stored in the global  variable
- Part of the PostgreSQL regression testing framework (pg_regress)
- The file format parsing is strict and will bail out on malformed entries