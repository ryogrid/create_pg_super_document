# DictSubState

## Location
src/include/tsearch/ts_public.h: 157 - 159

## Overview
DictSubState is a structure used to support complex dictionaries like thesaurus in PostgreSQL's text search system, providing state management for multi-step lexicalization processes.

## Definition
```c
typedef struct
{
    bool        isend;          /* in: marks for lexize_info about text end is
                                 * reached */
    bool        getnext;        /* out: dict wants next lexeme */
    void       *private_state;  /* internal dict state between calls with
                                 * getnext == true */
} DictSubState;
```

## Detailed Description
DictSubState serves as a communication and state management structure for complex dictionary operations in PostgreSQL's text search system. It is specifically designed to handle dictionaries that require multiple processing steps or need to maintain state between lexicalization calls, such as thesaurus dictionaries.

The structure acts as the fourth argument to the dictlexize method, enabling sophisticated dictionary implementations that cannot complete their work in a single function call. This is particularly important for dictionaries that need to look ahead in the text stream, perform complex transformations, or maintain context across multiple tokens.

The bidirectional communication is facilitated through the isend and getnext flags: the calling code uses isend to inform the dictionary when the end of input text is reached, while the dictionary uses getnext to request additional lexemes when it needs more context to make lexicalization decisions.

## Parameters / Member Variables
- `isend`: Input boolean flag that indicates to the dictionary whether the end of the input text has been reached
- `getnext`: Output boolean flag that the dictionary sets to true when it needs the next lexeme to continue processing
- `private_state`: Void pointer for storing internal dictionary state between function calls when getnext is true

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references - uses basic C types only)
- Called from (representative examples):
  - ts_lexize (src/backend/tsearch/dict.c:36)
  - thesaurus_lexize (src/backend/tsearch/dict_thesaurus.c:791)

## Notes and Other Information
- This structure is specifically designed for complex dictionaries that cannot complete lexicalization in a single call
- The private_state pointer allows dictionary implementations to maintain arbitrary internal state between calls
- The getnext mechanism enables dictionaries to request more input when needed for proper lexicalization
- Memory management of the private_state is the responsibility of the dictionary implementation
- This pattern is essential for implementing sophisticated linguistic processing like thesaurus expansion
- The structure enables streaming lexicalization where dictionaries can process text incrementally rather than requiring the entire input upfront