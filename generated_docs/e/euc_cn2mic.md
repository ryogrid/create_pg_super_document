# euc_cn2mic

## Location
src/backend/utils/mb/conversion_procs/euc_cn_and_mic/euc_cn_and_mic.c: 76 - 119

## Overview
Core conversion function that performs the actual character-by-character conversion from EUC-CN (Extended Unix Code for Chinese) encoding to MIC (Multi-byte Internal Code) format.

## Definition
```c
static int euc_cn2mic(const unsigned char *euc, unsigned char *p, int len, bool noError)
```

## Detailed Description
This static function implements the core algorithm for converting text from EUC-CN encoding to PostgreSQL's Multi-byte Internal Code (MIC) format. It processes the input byte by byte, handling both ASCII characters (single-byte) and Chinese characters (two-byte sequences). For Chinese characters, it adds the MIC language code LC_GB2312_80 as a prefix to identify the character set within the MIC encoding. The function includes error handling for invalid sequences and null-terminates the output string.

## Parameters / Member Variables
- `euc`: Pointer to the input buffer containing EUC-CN encoded text
- `p`: Pointer to the output buffer for MIC encoded text
- `len`: Length of the input text in bytes
- `noError`: Boolean flag indicating whether to suppress error reporting on invalid sequences

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - [report_invalid_encoding](../r/report_invalid_encoding.md) (error reporting function)
  - PG_EUC_CN (encoding constant)
  - LC_GB2312_80 (MIC language code for GB2312-80 character set)
- Called from:
  - [euc_cn_to_mic](euc_cn_to_mic.md) (PostgreSQL function wrapper)

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- Handles both ASCII (single-byte) and Chinese (two-byte) characters appropriately
- Chinese characters in EUC-CN are identified by having the high bit set in both bytes
- In MIC format, Chinese characters are prefixed with LC_GB2312_80 to identify the character set
- Returns the number of input bytes processed
- Includes validation for incomplete multibyte sequences and null bytes
- The function stops processing on error if noError is true, otherwise reports the encoding error
- Output is null-terminated for string safety