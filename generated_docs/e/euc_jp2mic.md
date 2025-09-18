# euc_jp2mic

## Location
src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c: 406 - 466

## Overview
Core conversion function that transforms Japanese EUC-JP (Extended Unix Code for Japanese) encoded text to PostgreSQL's Mule Internal Code (MIC) encoding, handling the EUC-JP character set structure.

## Definition


## Detailed Description
This function converts EUC-JP encoded Japanese text to Mule Internal Code format. EUC-JP uses a structured encoding scheme where different character sets are identified by specific byte patterns: ASCII characters (0x00-0x7F), JIS X0208 kanji and kana (high-bit set bytes), JIS X0201 katakana preceded by SS2 (0x8E), and JIS X0212 supplementary kanji preceded by SS3 (0x8F). The function identifies these patterns and adds appropriate MIC language character (LC) prefixes to distinguish the different Japanese character sets in the output.

## Parameters / Member Variables
- : Source string in EUC-JP encoding to be converted
- : Destination buffer where MIC encoded output will be written
- : Length of the source EUC-JP string in bytes  
- : Boolean flag indicating whether to suppress error reporting for invalid sequences

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET: Check if character has high bit set
  - pg_encoding_verifymbchar: Validate EUC-JP character sequence length
  - report_invalid_encoding: Report encoding conversion errors
  - SS2: Single Shift 2 byte (0x8E) indicating JIS X0201 katakana
  - SS3: Single Shift 3 byte (0x8F) indicating JIS X0212 characters
  - LC_JISX0201K: Language character code for JIS X0201 katakana
  - LC_JISX0208: Language character code for JIS X0208
  - LC_JISX0212: Language character code for JIS X0212
  - PG_EUC_JP: PostgreSQL encoding constant for EUC-JP
- Called from (representative examples):
  - euc_jp_to_mic: PostgreSQL function wrapper for EUC-JP to MIC conversion
  - PGEUCALTCODE: Referenced in encoding conversion system

## Notes and Other Information
- Handles ASCII characters (0x00-0x7F) by direct copying
- Processes EUC-JP's structured encoding using shift sequences (SS2, SS3)
- Maps JIS X0201 katakana (SS2 + 1 byte) to LC_JISX0201K prefix
- Maps JIS X0212 supplementary kanji (SS3 + 2 bytes) to LC_JISX0212 prefix  
- Maps regular JIS X0208 kanji/kana (2 bytes with high bits set) to LC_JISX0208 prefix
- Validates EUC-JP character sequences using pg_encoding_verifymbchar
- Returns the number of source bytes processed
- Located in src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c:406-466
- Implements straightforward EUC-JP to MIC mapping following the EUC structure