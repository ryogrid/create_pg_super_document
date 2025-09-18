# mic2euc_cn

## Location
[src/backend/utils/mb/conversion_procs/euc_cn_and_mic/euc_cn_and_mic.c:120-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_cn_and_mic/euc_cn_and_mic.c#L120-L166)

## Overview
Core conversion function that performs the actual character-by-character conversion from MIC (Multi-byte Internal Code) format to EUC-CN (Extended Unix Code for Chinese) encoding.

## Definition
```c
static int mic2euc_cn(const unsigned char *mic, unsigned char *p, int len, bool noError)
```

## Detailed Description
This static function implements the core algorithm for converting text from PostgreSQL's Multi-byte Internal Code (MIC) format to EUC-CN encoding. It processes the input byte by byte, handling both ASCII characters (single-byte) and Chinese characters (three-byte sequences in MIC). For Chinese characters, it expects the MIC language code LC_GB2312_80 as a prefix, which it strips while copying the two-byte character data to the output. The function includes comprehensive error handling for invalid sequences, untranslatable characters, and null-terminates the output string.

## Parameters / Member Variables
- `mic`: Pointer to the input buffer containing MIC encoded text
- `p`: Pointer to the output buffer for EUC-CN encoded text
- `len`: Length of the input text in bytes
- `noError`: Boolean flag indicating whether to suppress error reporting on invalid sequences

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - LC_GB2312_80 (MIC language code for GB2312-80 character set)
  - [report_untranslatable_char](../r/report_untranslatable_char.md) (error reporting function for unsupported character sets)
  - [report_invalid_encoding](../r/report_invalid_encoding.md) (error reporting function for malformed sequences)
  - PG_MULE_INTERNAL (MIC encoding constant)
  - PG_EUC_CN (EUC-CN encoding constant)
- Called from:
  - [mic_to_euc_cn](mic_to_euc_cn.md) (PostgreSQL function wrapper)

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit
- Handles both ASCII (single-byte) and Chinese (three-byte MIC sequences) characters
- Chinese characters in MIC format are identified by the LC_GB2312_80 language code prefix
- Validates that MIC multibyte sequences are exactly 3 bytes long with proper high-bit patterns
- Reports untranslatable characters when encountering unsupported MIC language codes
- Returns the number of input bytes processed
- Includes validation for incomplete multibyte sequences and null bytes
- The function stops processing on error if noError is true, otherwise reports appropriate encoding errors
- Output is null-terminated for string safety
- Performs the reverse conversion of euc_cn2mic function