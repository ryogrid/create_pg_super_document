# cmpLexeme

## Location
[src/backend/tsearch/dict_thesaurus.c:356-371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L356-L371)

## Overview
A static comparison function that compares two TheLexeme structures by their lexeme strings, handling NULL lexemes appropriately.

## Definition

```c
static int
cmpLexeme(const TheLexeme *a, const TheLexeme *b)
```
## Detailed Description
This function implements a comparison function for TheLexeme structures that can be used for sorting or searching operations. It performs a three-way comparison similar to strcmp, but with special handling for NULL lexemes. The function follows the standard comparison function convention where it returns negative, zero, or positive values to indicate the relative ordering of the two lexemes.

The comparison logic prioritizes NULL lexemes as "greater than" non-NULL lexemes, ensuring consistent ordering behavior when dealing with incomplete or uninitialized lexeme entries in the thesaurus dictionary processing.

## Parameters / Member Variables
- `a`: Pointer to the first TheLexeme structure to compare
- `b`: Pointer to the second TheLexeme structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function)
  - [TheLexeme](../T/TheLexeme.md) (structure type)
- Called from (representative examples):
  - [cmpLexemeQ](cmpLexemeQ.md)
  - [cmpTheLexeme](cmpTheLexeme.md)  
  - [compileTheLexeme](compileTheLexeme.md)

## Notes and Other Information
- Returns 0 if both lexemes are NULL or if both lexemes are identical strings
- Returns 1 if the first lexeme is NULL and the second is not (NULL > non-NULL)
- Returns -1 if the second lexeme is NULL and the first is not (non-NULL < NULL)
- For non-NULL lexemes, delegates to strcmp for standard string comparison
- This function is used internally within the thesaurus dictionary implementation for maintaining sorted lexeme lists and performing efficient lookups

## Simplified Source

```c
static int cmpLexeme(const TheLexeme *a, const TheLexeme *b) {
    // Handle null lexeme cases
    if (a->lexeme == NULL) {
        if (b->lexeme == NULL)
            return 0;    // Both null - equal
        else
            return 1;    // a is null, b is not - a > b
    }
    else if (b->lexeme == NULL) {
        return -1;       // b is null, a is not - a < b
    }

    // Both have lexemes - compare strings
    return strcmp(a->lexeme, b->lexeme);
}
```