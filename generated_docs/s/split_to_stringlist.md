# split_to_stringlist

## Location
[src/test/regress/pg_regress.c:234-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L234-L253)

## Overview
A utility function that parses a delimited string and converts it into a `_stringlist` linked list with each token as a separate node.

## Definition
```c
static void split_to_stringlist(const char *s, const char *delim, _stringlist **listhead)
```

## Detailed Description
This function tokenizes a delimited string using the standard C library `strtok` function. It creates a working copy of the input string (since `strtok` modifies the string), then iteratively extracts tokens using the specified delimiter(s) and adds each token to the string list using `add_stringlist_item`. After processing all tokens, it frees the working copy to prevent memory leaks. This is commonly used to parse comma-separated lists or other delimited configuration values.

## Parameters / Member Variables
- `s`: Input string to be split (const char pointer)
- `delim`: String containing delimiter characters (const char pointer) 
- `listhead`: Double pointer to the head of the `_stringlist` where tokens will be appended

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) - PostgreSQL string duplication function
  - `strtok` - Standard C library string tokenization function
  - [add_stringlist_item](../a/add_stringlist_item.md) - Function to append items to the string list
  - `free` - Standard C library memory deallocation function
  - `[_stringlist](_stringlist.md)` - Structure type for linked list nodes
- Called from (representative examples):
  - [regression_main](../r/regression_main.md) - in pg_regress for parsing test lists and configuration options

## Notes and Other Information
- This is a static function local to `src/test/regress/pg_regress.c`
- Uses `strtok` which treats any character in the `delim` string as a delimiter
- Creates a temporary copy of the input string to avoid modifying the original
- Tokens are added to the list in the order they appear in the input string
- Handles multiple consecutive delimiters by treating them as separating empty tokens (standard `strtok` behavior)
- Essential for parsing command-line arguments and configuration files in the regression test framework

## Simplified Source

```c
static void split_to_stringlist(const char *s, const char *delim, _stringlist **listhead) {
    // Create working copy since strtok modifies the string
    char *copy = pg_strdup(s);

    // Tokenize and add each token to the list
    char *token = strtok(copy, delim);
    while (token) {
        add_stringlist_item(listhead, token);
        token = strtok(NULL, delim);
    }

    // Clean up working copy
    free(copy);
}
```