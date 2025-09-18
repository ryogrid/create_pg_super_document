# dispell_init

## Location
src/backend/tsearch/dict_ispell.c: 30 - 110

## Overview
Initializes an Ispell dictionary object for text search by parsing configuration parameters and loading dictionary, affix, and stopword files.

## Definition
```c
Datum dispell_init(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dispell_init` function is the initialization routine for PostgreSQL's Ispell text search dictionary. It processes a list of configuration parameters to set up a `DictISpell` structure that contains the necessary data for performing morphological analysis and word normalization. The function handles three types of configuration files:

1. **Dictionary file** (`dictfile`): Contains the base word forms and their variations
2. **Affix file** (`afffile`): Contains prefix and suffix rules for word transformation
3. **Stopwords file** (`stopwords`): Contains words to be filtered out during lexical analysis

The function ensures that both dictionary and affix files are provided (as they are mandatory), while stopwords are optional. After loading the files, it sorts the dictionary and affix data structures to optimize lookup performance.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `dictoptions`: A `List` pointer containing `DefElem` structures with configuration parameters

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md): Allocates zero-initialized memory
  - [NIStartBuild](../N/NIStartBuild.md): Initializes the NIspell object structure
  - [NISortDictionary](../N/NISortDictionary.md): Sorts dictionary entries for optimal lookup
  - [NISortAffixes](../N/NISortAffixes.md): Sorts affix rules for optimal lookup  
  - [NIFinishBuild](../N/NIFinishBuild.md): Finalizes the NIspell object after loading
  - [NIImportDictionary](../N/NIImportDictionary.md): Loads dictionary data from file
  - `NIImportAffixes`: Loads affix rules from file
  - [get_tsearch_config_filename](../g/get_tsearch_config_filename.md): Resolves configuration file paths
  - [defGetString](defGetString.md): Extracts string values from DefElem structures
  - [readstoplist](../r/readstoplist.md): Loads stopword list from file
  - [lowerstr](../l/lowerstr.md): Function for lowercase string processing
  - `DictISpell`: Main dictionary structure type
  - [DefElem](../D/DefElem.md): Configuration element structure
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL function call mechanism)

## Notes and Other Information
- This function is designed to be called through PostgreSQL's function call interface as part of text search dictionary initialization
- The function performs comprehensive error checking, ensuring no duplicate parameters are provided
- Both `dictfile` and `afffile` parameters are mandatory; the function will error if either is missing
- The `stopwords` parameter is optional and can be omitted
- File paths are resolved using PostgreSQL's text search configuration directory structure
- Memory allocation uses PostgreSQL's memory management system (`palloc0`)
- The function returns a `Datum` containing a pointer to the initialized `DictISpell` structure