# addToResult

## Location
[src/backend/tsearch/spell.c:2161-2175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2161-L2175)

## Overview
Adds a word to the result array of normalized forms, preventing duplicates and managing array bounds.

## Definition
```c
static int addToResult(char **forms, char **cur, char *word)
```

## Detailed Description
addToResult is a utility function that manages the collection of normalized word forms during the spell checking process. It adds a new word to the results array only if it's different from the previous entry (to avoid duplicates) and if there's space available in the array. The function maintains the array in a sorted manner and ensures proper null termination.

The function performs bounds checking against MAX_NORM to prevent buffer overflow and uses string comparison to detect duplicate entries. It allocates memory for each new word using pstrdup to ensure the result persists beyond the current function scope.

## Parameters / Member Variables
- `forms`: Base pointer to the array of normalized word forms
- `cur`: Current position pointer in the forms array
- `word`: Word to be added to the result array

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication function)
  - strcmp (string comparison function)
  - MAX_NORM (maximum number of normalized forms constant, value 1024)
- Called from (representative examples):
  - [NormalizeSubWord](../N/NormalizeSubWord.md) (3 times at lines 2221, 2246, 2266)

## Notes and Other Information
- Returns 1 if the word was successfully added, 0 if not added (duplicate or array full)
- Prevents duplicate entries by comparing with the previous word in the array
- Maintains null termination of the result array
- Part of PostgreSQL's text search spell checking functionality
- The MAX_NORM limit of 1024 prevents excessive memory usage during normalization

## Simplified Source

```c
static int
addToResult(char **forms, char **cur, char *word)
{
    // Check if array is full
    if (cur - forms >= MAX_NORM - 1)
        return 0;

    // Add word only if different from previous entry (avoid duplicates)
    if (forms == cur || strcmp(word, *(cur - 1)) != 0) {
        *cur = pstrdup(word);
        *(cur + 1) = NULL;  // Maintain null termination
        return 1;
    }

    return 0;
}
```