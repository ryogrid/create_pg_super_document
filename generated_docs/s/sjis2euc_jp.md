# sjis2euc_jp

## Location
src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c: 638 - 772

## Overview
Converts text from Shift JIS (SJIS) encoding to EUC-JP (Extended Unix Code for Japanese) encoding, handling various Japanese character sets including ASCII, JIS X0201 kana, JIS X0208 kanji, and IBM extended characters.

## Definition
static int sjis2euc_jp(const unsigned char *sjis, unsigned char *p, int len, bool noError)

## Detailed Description
This function performs character-by-character conversion from Shift JIS to EUC-JP encoding. It processes different categories of Japanese characters:

- **ASCII characters**: Copied directly without conversion
- **JIS X0201 half-width katakana**: Characters in range 0xa1-0xdf, converted by adding SS2 prefix
- **JIS X0208 kanji**: Main Japanese character set, converted using standard SJIS to EUC mathematical transformation
- **NEC selection IBM kanji**: Special IBM character extensions (0xed40-0xf040 range) handled via lookup table
- **User-defined characters**: Two ranges (UDC1: 0xf040-0xf540, UDC2: 0xf540-0xfa40) mapped to EUC-JP extended areas
- **IBM kanji**: Characters ≥0xfa40 mapped through lookup table to either JIS X0208 or JIS X0212

The function includes comprehensive error handling and supports both strict and lenient conversion modes.

## Parameters / Member Variables
- : Source buffer containing SJIS encoded text to convert
- : Destination buffer where the converted EUC-JP text will be written  
- : Number of bytes remaining in the source buffer to process
- : If true, stops conversion at first invalid sequence; if false, reports encoding errors via report_invalid_encoding

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - [report_invalid_encoding](../r/report_invalid_encoding.md)
  - [pg_encoding_verifymbchar](../p/pg_encoding_verifymbchar.md)
  - PG_SJIS
  - SS2 (Single Shift 2 - prefix for JIS X0201 kana in EUC-JP)
  - SS3 (Single Shift 3 - prefix for JIS X0212 kanji in EUC-JP)
  - PGEUCALTCODE (alternative character code for unmappable characters)
  - ibmkanji (lookup table for IBM extended characters with .nec, .sjis, and .euc fields)
- Called from (representative examples):
  - [sjis_to_euc_jp](sjis_to_euc_jp.md) (public conversion function)

## Notes and Other Information
- The function null-terminates the output buffer
- Returns the number of bytes processed from the input buffer
- Handles multi-byte character boundary validation using pg_encoding_verifymbchar
- Uses mathematical formulas for standard JIS X0208 character conversion: ((c1 & 0x3f) << 1) + 0x9f + (c2 > 0x9e)
- **UDC1 mapping**: SJIS 0xf040-0xf540 maps to EUC-JP X0208 85-94 ku (0xf5a1-0xfefe)
- **UDC2 mapping**: SJIS 0xf540-0xfa40 maps to EUC-JP X0212 85-94 ku (SS3 + 0xf5a1-0xfefe)
- **NEC selection**: Special handling for NEC-specific IBM kanji characters in 0xeb40-0xf040 range
- **IBM kanji lookup**: Uses ibmkanji array to map IBM extended characters, with special handling for X0212 characters (indicated by high byte 0x8f)
- Unmappable characters are replaced with PGEUCALTCODE placeholder
- The conversion preserves character boundaries and validates input encoding integrity