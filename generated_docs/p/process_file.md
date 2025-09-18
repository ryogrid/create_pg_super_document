# process_file

## Location
src/bin/psql/command.c: 4380 - 4446

## Overview
A utility function in pgbench that reads a file containing SQL script content and adds it to the list of scripts to be executed during benchmarking.

## Definition


## Detailed Description
The  function is responsible for reading script files in pgbench, PostgreSQL's built-in benchmarking tool. It handles file I/O operations to read SQL script content from either a specified file or stdin (when filename is "-"), then parses the content and adds it to the script collection with the specified weight. The function includes proper error handling for file operations and memory management for the file contents.

## Parameters / Member Variables
- : Path to the script file to be processed. Special value "-" indicates reading from stdin
- : Numeric weight value assigned to this script, affecting its selection probability during benchmark execution

## Dependencies
- Functions called/Symbols referenced:
  - fopen (standard library function for file opening)
  - read_file_contents (pgbench utility to read entire file into memory)
  - ParseScript (pgbench function to parse and register the script)
- Called from (representative examples):
  - main (in pgbench.c for processing command-line specified script files)
  - exec_command_include (in psql for including files)

## Notes and Other Information
- The function is declared as static, limiting its scope to the pgbench.c compilation unit
- Includes robust error handling with pg_fatal calls for file operation failures
- Automatically handles both regular files and stdin input transparently
- Memory management is properly handled with free() call after script parsing
- The filename parameter storage must persist as noted in the comment, suggesting the filename string is retained by ParseScript