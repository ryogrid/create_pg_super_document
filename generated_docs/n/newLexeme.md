# newLexeme

## Location
src/backend/tsearch/dict_thesaurus.c: 72 - 105

## Overview
Creates and initializes a new lexeme entry in the thesaurus dictionary structure with associated substitution information.

## Definition


## Detailed Description
The newLexeme function is responsible for adding a new lexeme (word/token) to the DictThesaurus structure. It dynamically manages memory allocation for the lexeme array, automatically expanding the storage capacity when needed. The function extracts the lexeme string from a character buffer range, creates a copy in allocated memory, and initializes associated metadata including substitution ID and position information.

The function implements a dynamic array growth strategy, starting with an initial capacity of 16 entries and doubling the size when capacity is exceeded. Each lexeme is stored with its own LexemeInfo structure that tracks substitution relationships used by the thesaurus dictionary.

## Parameters / Member Variables
- : Pointer to the DictThesaurus structure where the new lexeme will be added
- : Pointer to the beginning of the lexeme string in the source buffer
- : Pointer to the end of the lexeme string in the source buffer (exclusive)
- : Unique identifier for the substitution rule this lexeme belongs to
- : Position index of this lexeme within its substitution rule

## Dependencies
- Functions called/Symbols referenced:
  - palloc: PostgreSQL memory allocation function
  - repalloc: PostgreSQL memory reallocation function
  - memcpy: Standard C library function for memory copying
  - DictThesaurus: The main thesaurus dictionary structure
  - TheLexeme: Structure representing individual lexeme entries
  - LexemeInfo: Structure containing lexeme metadata and substitution information

- Called from (representative examples):
  - thesaurusRead: Main parsing function that processes thesaurus configuration files

## Notes and Other Information
- This is a static function, only accessible within the dict_thesaurus.c file
- The function assumes that the input string range [b,e) is valid and properly null-terminates the copied lexeme
- Memory allocation uses PostgreSQL's palloc/repalloc functions which provide error handling and memory context management
- The dynamic array growth strategy (doubling) provides O(1) amortized insertion time
- Each lexeme maintains a linked list of LexemeInfo entries to support multiple substitution rules