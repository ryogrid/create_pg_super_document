# LexemeInfo

## Location
[src/backend/tsearch/dict_thesaurus.c:30-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L30-L37)

## Overview
LexemeInfo is a structure used in PostgreSQL's thesaurus dictionary implementation to store metadata about lexemes and their relationships within thesaurus entries and variants.

## Definition
```c
typedef struct LexemeInfo
{
    uint32      idsubst;        /* entry's number in DictThesaurus->subst */
    uint16      posinsubst;     /* pos info in entry */
    uint16      tnvariant;      /* total num lexemes in one variant */
    struct LexemeInfo *nextentry;
    struct LexemeInfo *nextvariant;
} LexemeInfo;
```

## Detailed Description
LexemeInfo serves as a node in a linked list structure that organizes lexemes within the thesaurus dictionary system. It maintains references to substitute entries and tracks position information for lexeme matching and substitution operations. The structure supports both entry-level and variant-level linking, enabling efficient traversal of related lexemes during thesaurus lookups.

This structure is primarily used in `src/backend/tsearch/dict_thesaurus.c` for implementing thesaurus-based text search functionality, where it helps organize and navigate through synonym relationships and lexeme variants.

## Parameters / Member Variables
- `idsubst`: Entry number reference into the DictThesaurus->subst array, identifying which substitution rule this lexeme belongs to
- `posinsubst`: Position information within the substitution entry, used for ordering and matching lexemes
- `tnvariant`: Total number of lexemes in the current variant, used for variant boundary detection during traversal
- `nextentry`: Pointer to the next LexemeInfo in the same entry, forming a linked list of related lexemes
- `nextvariant`: Pointer to the next LexemeInfo in a different variant, enabling traversal across lexeme variants

## Dependencies
- Functions called/Symbols referenced:
  - [LexemeInfo](LexemeInfo.md) (self-referential for linked list pointers)
- Called from (representative examples):
  - [newLexeme](../n/newLexeme.md)
  - [addCompiledLexeme](../a/addCompiledLexeme.md)
  - [cmpLexemeInfo](../c/cmpLexemeInfo.md)
  - [thesaurus_init](../t/thesaurus_init.md)
  - [matchIdSubst](../m/matchIdSubst.md)
  - [findVariant](../f/findVariant.md)
  - [checkMatch](../c/checkMatch.md)
  - [thesaurus_lexize](../t/thesaurus_lexize.md)

## Notes and Other Information
- The structure implements a doubly-linked organizational system where entries and variants can be traversed independently
- Used extensively in thesaurus dictionary compilation and lookup operations
- Memory allocation for LexemeInfo instances is handled through PostgreSQL's palloc() memory management system
- The structure is part of the larger thesaurus dictionary framework that includes TheLexeme, TheSubstitute, and DictThesaurus structures