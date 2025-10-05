# ScanECPGKeywordLookup

## Location
[src/interfaces/ecpg/preproc/ecpg_keywords.c:39-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/ecpg_keywords.c#L39-L54)

## Overview
ScanECPGKeywordLookup is a keyword lookup function for PostgreSQL's embedded SQL (ECPG) preprocessor that determines if a given text string is a reserved keyword and returns its corresponding token value.

## Definition
```c
int ScanECPGKeywordLookup(const char *text)
```

## Detailed Description
This function performs keyword lookup for the ECPG (Embedded SQL in C for PostgreSQL) preprocessor. It implements a two-tier keyword recognition system that first checks for standard SQL keywords defined by the PostgreSQL backend, and then checks for ECPG-specific keywords. The function uses the same case-folding rules as the backend for consistent keyword matching behavior.

The lookup process follows this hierarchy:
1. First, it searches for the text in the standard SQL keywords using ScanKeywordLookup with the ScanKeywords table
2. If found, it returns the corresponding token from SQLScanKeywordTokens array
3. If not found in standard SQL keywords, it searches in ECPG-specific keywords using ScanECPGKeywords table
4. If found in ECPG keywords, it returns the corresponding token from ECPGScanKeywordTokens array
5. If no match is found in either table, it returns -1

## Parameters / Member Variables
- `text`: A null-terminated string containing the word to be checked for keyword status. The function applies case-folding rules during comparison.

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeywordLookup](ScanKeywordLookup.md) (called twice - once for SQL keywords, once for ECPG keywords)
  - ScanKeywords (keyword table for standard SQL keywords)
  - ScanECPGKeywords (keyword table for ECPG-specific keywords)
  - SQLScanKeywordTokens (token array for SQL keywords)
  - ECPGScanKeywordTokens (token array for ECPG keywords)
- Called from (representative examples):
  - No direct references found in the analyzed codebase (likely called by lexical analyzer components)

## Notes and Other Information
- Returns -1 if the input text is not a recognized keyword
- Returns a positive token value if the text matches a keyword
- Part of the ECPG preprocessor infrastructure located in src/interfaces/ecpg/preproc/
- The function prioritizes standard SQL keywords over ECPG-specific keywords in the lookup order
- Uses case-folding rules consistent with PostgreSQL backend for keyword matching
- The ECPGScanKeywordTokens array is generated from ecpg_kwlist.h using preprocessor macros

## Simplified Source

```c
int
ScanECPGKeywordLookup(const char *text)
{
    int kwnum;

    // First check standard SQL keywords from backend
    kwnum = ScanKeywordLookup(text, &ScanKeywords);
    if (kwnum >= 0)
        return SQLScanKeywordTokens[kwnum];

    // Then check ECPG-specific keywords
    kwnum = ScanKeywordLookup(text, &ScanECPGKeywords);
    if (kwnum >= 0)
        return ECPGScanKeywordTokens[kwnum];

    // Not a keyword
    return -1;
}
```