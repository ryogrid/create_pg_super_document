# mic2sjis

## Location
src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c: 299 - 405

## Overview
Core conversion function that transforms PostgreSQL's Mule Internal Code (MIC) encoded text to Japanese Shift JIS (SJIS) encoding, handling various Japanese character sets and user-defined character areas.

## Definition


## Detailed Description
This function performs the reverse conversion of sjis2mic, transforming MIC encoded Japanese text back to Shift JIS format. It processes MIC language character codes to identify different Japanese character sets (JIS X0201 katakana, JIS X0208 kanji/kana, JIS X0212 supplementary kanji) and converts them to their corresponding SJIS byte sequences. The function handles user-defined character areas (UDC1 and UDC2) and uses lookup tables for IBM kanji mappings. It includes proper validation of MIC character sequences and error handling for untranslatable characters.

## Parameters / Member Variables
- : Source string in Mule Internal Code encoding to be converted
- : Destination buffer where SJIS encoded output will be written  
- : Length of the source MIC string in bytes
- : Boolean flag indicating whether to suppress error reporting for invalid sequences

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET: Check if character has high bit set
  - pg_encoding_verifymbchar: Validate MIC character sequence length
  - report_invalid_encoding: Report encoding conversion errors
  - report_untranslatable_char: Report characters that cannot be converted
  - LC_JISX0201K: Language character code for JIS X0201 katakana
  - LC_JISX0208: Language character code for JIS X0208
  - LC_JISX0212: Language character code for JIS X0212
  - PGSJISALTCODE: Alternative SJIS code for unmappable characters
  - ibmkanji: Lookup table for IBM kanji mappings
  - PG_MULE_INTERNAL: PostgreSQL encoding constant for MIC
  - PG_SJIS: PostgreSQL encoding constant for Shift JIS
- Called from (representative examples):
  - mic_to_sjis: PostgreSQL function wrapper for MIC to SJIS conversion
  - PGEUCALTCODE: Referenced in encoding conversion system

## Notes and Other Information
- Handles ASCII characters (0x00-0x7F) by direct copying
- Processes JIS X0201 single-byte katakana by removing LC prefix
- Converts JIS X0208 characters using algorithmic transformation with special handling for UDC1 range
- Maps JIS X0212 characters through IBM kanji lookup table or handles UDC2 range
- Uses alternative codes (PGSJISALTCODE) for characters that cannot be mapped
- Validates MIC character sequences using pg_encoding_verifymbchar
- Returns the number of source bytes processed
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:299-405
- Implements comprehensive MIC to SJIS mapping with proper error handling and validation