# tokenize_expand_file

## Location
[src/backend/libpq/hba.c:493-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L493-L569)

## Overview
Expands a file referenced by '@' directive within an HBA configuration field into a flat list of tokens that are appended to the existing token list.

## Definition

```c
static List *
tokenize_expand_file(List *tokens,
					 const char *outer_filename,
					 const char *inc_filename,
					 int elevel,
					 int depth,
					 char **err_msg)
```
## Detailed Description
This function handles file expansion within HBA configuration fields when a token beginning with '@' is encountered. Unlike tokenize_include_file which processes entire include directives, this function processes a file referenced within a field and flattens all its tokens into the current field's token list. It opens the referenced file, tokenizes its entire contents, and then extracts all individual tokens from every line and field, appending them to the existing tokens list. This enables constructs like "foo,bar,@filename" to work as expected, where @filename expands to multiple comma-separated values. The function handles recursive expansion, proper memory context management, and comprehensive error propagation.

## Parameters / Member Variables
- : Existing list of AuthToken structures to which new tokens will be appended
- : Path of the file containing the '@' reference (used for relative path resolution)
- : Path of the file to be expanded (may be relative or absolute)
- : Error reporting level for ereport calls (e.g., ERROR, LOG, WARNING)
- : Current recursion depth for nested expansions (prevents infinite recursion)
- : Pointer to store error message string if expansion fails

## Dependencies
- Functions called/Symbols referenced:
  - AbsoluteConfigLocation: Resolves relative file paths to absolute paths
  - open_auth_file: Opens authentication configuration files with error handling
  - tokenize_auth_file: Processes the included file into TokenizedAuthLine structures
  - free_auth_file: Closes file and cleans up resources
  - lappend: Adds tokens to the result list
  - MemoryContextSwitchTo: Manages memory allocation context
  - pstrdup: Duplicates error message strings
  - pfree: Frees allocated memory
- Called from (representative examples):
  - next_token: When processing '@' file references within fields
  - match_auth_token: During token matching with file expansion

## Notes and Other Information
- Returns the modified tokens list with new tokens appended
- Supports recursive expansion if the included file contains '@' references or include directives
- Flattens multi-line, multi-field file contents into a single token list
- Proper error propagation - stops on first error encountered in any line
- Uses tokenize_context memory context for all token allocations
- Enables flexible configuration patterns like comma-separated lists spanning multiple files
- Part of PostgreSQL's authentication configuration expansion system
- Handles complex nested structures by iterating through lines, fields, and tokens

## Simplified Source

```c
// Simplified version of tokenize_expand_file
static List *tokenize_expand_file(List *tokens,
                                  const char *outer_filename,
                                  const char *inc_filename,
                                  int elevel,
                                  int depth,
                                  char **err_msg) {
    // Resolve file path and open the included file
    char *inc_fullname = AbsoluteConfigLocation(inc_filename, outer_filename);
    FILE *inc_file = open_auth_file(inc_fullname, elevel, depth, err_msg);

    if (inc_file == NULL) {
        pfree(inc_fullname);
        return tokens;
    }

    // Tokenize the entire included file
    List *inc_lines = NIL;
    tokenize_auth_file(inc_fullname, inc_file, &inc_lines, elevel, depth);
    pfree(inc_fullname);

    // Extract all tokens from all lines and fields
    foreach(inc_line, inc_lines) {
        TokenizedAuthLine *tok_line = (TokenizedAuthLine *) lfirst(inc_line);

        // Stop on first error encountered
        if (tok_line->err_msg) {
            *err_msg = pstrdup(tok_line->err_msg);
            break;
        }

        // Process each field in the line
        foreach(inc_field, tok_line->fields) {
            List *inc_tokens = lfirst(inc_field);

            // Add each token to the result list
            foreach(inc_token, inc_tokens) {
                AuthToken *token = lfirst(inc_token);

                // Switch to proper memory context for lappend
                MemoryContext oldcxt = MemoryContextSwitchTo(tokenize_context);
                tokens = lappend(tokens, token);
                MemoryContextSwitchTo(oldcxt);
            }
        }
    }

    free_auth_file(inc_file, depth);
    return tokens;
}
```

Key simplifications made:
- Added clear comments for each major processing step
- Streamlined the nested loop structure with descriptive comments
- Preserved the memory context management and error handling
- Maintained the file expansion and token flattening logic