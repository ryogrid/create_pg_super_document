# sjis2mic

## Location
src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c: 160 - 298

## Overview
Core conversion function that transforms Japanese Shift JIS (SJIS) encoded text to PostgreSQL's Mule Internal Code (MIC) encoding, handling various Japanese character sets including JIS X0208, X0212, and user-defined characters.

## Definition


## Detailed Description
This function performs the complex conversion from Shift JIS encoding to Mule Internal Code. It processes different types of Japanese characters including single-byte half-width katakana (JIS X0201), double-byte kanji and hiragana/katakana (JIS X0208), supplementary kanji (JIS X0212), and various user-defined character areas. The function also handles special IBM kanji mappings and NEC selection characters. It uses lookup tables and algorithmic conversion to map SJIS byte sequences to appropriate MIC character codes with proper language character (LC) prefixes.

## Parameters / Member Variables
- : Source string in Shift JIS encoding to be converted
- : Destination buffer where MIC encoded output will be written
- : Length of the source SJIS string in bytes
- : Boolean flag indicating whether to suppress error reporting for invalid sequences

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET: Check if character has high bit set
  - ISSJISHEAD: Validate SJIS lead byte
  - ISSJISTAIL: Validate SJIS trail byte
  - [report_invalid_encoding](../r/report_invalid_encoding.md): Report encoding conversion errors
  - LC_JISX0201K: Language character code for JIS X0201 katakana
  - LC_JISX0208: Language character code for JIS X0208
  - LC_JISX0212: Language character code for JIS X0212
  - PGEUCALTCODE: Alternative encoding code for unmappable characters
  - ibmkanji: Lookup table for IBM kanji mappings
- Called from (representative examples):
  - [sjis_to_mic](sjis_to_mic.md): PostgreSQL function wrapper for SJIS to MIC conversion
  - PGEUCALTCODE: Referenced in encoding conversion system

## Notes and Other Information
- Handles multiple Japanese character encoding standards within SJIS
- Supports conversion of JIS X0201 single-byte katakana (0xa1-0xdf range)
- Processes JIS X0208 kanji and kana using algorithmic conversion
- Maps user-defined characters (UDC1, UDC2) to appropriate JIS character sets
- Includes special handling for IBM kanji extensions and NEC selection characters
- Uses language character prefixes in MIC to identify different character sets
- Returns the number of source bytes processed
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:160-298
- Implements comprehensive SJIS to MIC mapping with error handling for malformed sequences