# tokenize_auth_file

## Location
[src/backend/libpq/hba.c:686-918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L686-L918)

## Overview
The central tokenization function that parses authentication configuration files into structured TokenizedAuthLine objects for further processing.

## Definition
```c
void tokenize_auth_file(const char *filename, FILE *file, List **tok_lines, int elevel, int depth)
```

## Detailed Description
The tokenize_auth_file function is the core parser for PostgreSQL's authentication configuration files (pg_hba.conf and pg_ident.conf). It reads the file line by line, tokenizes each line into structured data, and handles special directives like include statements.

Key functionality includes:

1. **Line-by-Line Processing**: Reads each line from the file, handling backslash continuations
2. **Field Tokenization**: Parses each line into fields using next_field_expand()
3. **Include Directive Handling**: Processes include, include_dir, and include_if_exists directives for recursive file inclusion
4. **Error Context Management**: Sets up error context callbacks to provide detailed location information for parsing errors
5. **Memory Management**: Uses dedicated memory contexts (tokenize_context and local linecxt) for efficient cleanup
6. **TokenizedAuthLine Creation**: Converts parsed data into TokenizedAuthLine structures for the output list

The function operates recursively when processing include directives, maintaining proper depth tracking and memory context management throughout the process.

## Parameters
- `filename`: Absolute path to the authentication configuration file being tokenized
- `file`: Already-opened FILE pointer to the configuration file
- `tok_lines`: Output parameter receiving the list of TokenizedAuthLine structures
- `elevel`: Error reporting level (e.g., ERROR, WARNING, LOG)
- `depth`: Current recursion depth for include file processing

## Dependencies
- Functions called/Symbols referenced:
  - [tokenize_error_callback](tokenize_error_callback.md)
  - AllocSetContextCreate
  - [pg_get_line_append](../p/pg_get_line_append.md)
  - [pg_strip_crlf](../p/pg_strip_crlf.md)
  - [next_field_expand](../n/next_field_expand.md)
  - [tokenize_include_file](tokenize_include_file.md)
  - [GetConfFilesInDir](../G/GetConfFilesInDir.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - ALLOCSET_SMALL_SIZES
  - CONF_FILE_START_DEPTH
- Data structures used:
  - [TokenizedAuthLine](../T/TokenizedAuthLine.md)
  - [AuthToken](../A/AuthToken.md)
  - [tokenize_error_callback_arg](tokenize_error_callback_arg.md)
- Called from (representative examples):
  - [tokenize_include_file](tokenize_include_file.md)
  - [tokenize_expand_file](tokenize_expand_file.md)
  - [load_hba](../l/load_hba.md)
  - [load_ident](../l/load_ident.md)
  - [fill_hba_view](../f/fill_hba_view.md)
  - [fill_ident_view](../f/fill_ident_view.md)

## Notes and Other Information
- All tokenization work is performed in dedicated memory contexts for efficient cleanup, critical for postmaster reloads
- Handles backslash line continuations by concatenating lines until a non-backslash-terminated line is found
- Supports three include directives: 'include' (required file), 'include_dir' (all files in directory), and 'include_if_exists' (optional file)
- Empty lines and comment-only lines are skipped and not added to the output token list
- Error messages are captured and stored in TokenizedAuthLine.err_msg field rather than immediately terminating processing
- The tokenize_context global memory context must be established before calling this function
- Recursive include processing maintains proper error context and line numbering across all included files