# TheLexeme

## Location
[src/backend/tsearch/dict_thesaurus.c:39-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L39-L43)

## Overview
TheLexeme is a structure used in PostgreSQL's thesaurus dictionary implementation to represent individual lexemes (words or terms) along with their associated lexeme information entries.

## Definition

```c
typedef struct
{
	char	   *lexeme;
	LexemeInfo *entries;
} TheLexeme;
```
## Detailed Description
TheLexeme serves as a fundamental data structure in the thesaurus dictionary functionality within PostgreSQL's text search system. It encapsulates a lexeme (a string representing a word or term) and maintains a pointer to associated LexemeInfo structures that contain metadata and variant information for that lexeme. This structure is used during thesaurus dictionary processing to organize and access lexeme data efficiently.

## Parameters / Member Variables
- `lexeme`: A character pointer to the string representation of the lexeme (word or term)
- `entries`: A pointer to LexemeInfo structures containing associated information and variants for this lexeme

## Dependencies
- Functions called/Symbols referenced:
  - TSLexeme
- Called from (representative examples):
  - [TheSubstitute](TheSubstitute.md)
  - [newLexeme](../n/newLexeme.md)
  - [thesaurusRead](../t/thesaurusRead.md)
  - [addCompiledLexeme](../a/addCompiledLexeme.md)
  - [cmpLexeme](../c/cmpLexeme.md)
  - [cmpLexemeQ](../c/cmpLexemeQ.md)
  - [cmpTheLexeme](../c/cmpTheLexeme.md)
  - [compileTheLexeme](../c/compileTheLexeme.md)
  - [findTheLexeme](../f/findTheLexeme.md)

## Notes and Other Information
- This structure is defined in src/backend/tsearch/dict_thesaurus.c at lines 39-43
- It works in conjunction with LexemeInfo structures to provide a complete lexeme representation system
- Used extensively throughout the thesaurus dictionary implementation for lexeme management and lookup operations
- Part of PostgreSQL's full-text search infrastructure