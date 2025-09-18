# compileTheSubstitute

## Location
src/backend/tsearch/dict_thesaurus.c: 502 - 595

## Overview
Processes and compiles substitute phrase entries in a thesaurus dictionary by normalizing them through a subdictionary and preparing them for runtime substitution operations.

## Definition


## Detailed Description
This function performs the compilation phase for thesaurus substitute phrases, which are the replacement text that will be returned when input phrases match thesaurus rules. It processes each substitute entry through the subdictionary to normalize the lexemes, handles special flags, and manages dynamic memory allocation for variable-length results.

The compilation process includes several key operations:
1. **Lexeme normalization**: Each substitute lexeme is processed through the subdictionary unless marked with DT_USEASIS flag
2. **Dynamic array management**: Uses repalloc to grow the result array as needed to accommodate variable numbers of lexemes returned by the subdictionary
3. **Flag handling**: Preserves special flags like DT_USEASIS to bypass subdictionary processing and TSL_ADDPOS for position information
4. **Error validation**: Ensures substitute phrases are not empty and that all lexemes are recognized by the subdictionary
5. **Memory management**: Replaces original substitute arrays with compiled versions and frees temporary storage

The function is essential for preparing efficient substitute phrase matching during thesaurus query processing.

## Parameters / Member Variables
- `d`: Pointer to the DictThesaurus structure containing the raw substitute phrase data to be compiled

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCall4 (calls subdictionary lexize function)  
  - repalloc (dynamic memory reallocation)
  - pstrdup (string duplication)
  - palloc (memory allocation)
  - pfree (memory deallocation)
  - TSLexeme, DictThesaurus (structure types)
  - DT_USEASIS (flag to bypass lexizing)
  - TSL_ADDPOS (flag for position information)
- Called from (representative examples):
  - thesaurus_init

## Notes and Other Information
- Handles the DT_USEASIS flag to allow literal substitute text without subdictionary normalization
- Implements dynamic array growth with doubling strategy for efficiency
- Sets TSL_ADDPOS flags appropriately to maintain position information in multi-lexeme substitutes
- Provides comprehensive error reporting with rule numbers for debugging thesaurus configuration
- Critical for thesaurus performance as it pre-processes all substitute phrases during initialization
- Ensures substitute phrases are never empty and all constituent lexemes are valid
- The compiled substitute arrays enable efficient phrase substitution during query processing
- Memory-efficient approach that replaces original arrays in-place and manages variable-length results dynamically