# is_valid_xml_namechar

## Location
[src/backend/utils/adt/xml.c:2364-2379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2364-L2379)

## Overview
A static validation function that determines whether a Unicode character is valid as any character (non-first) in an XML name according to XML naming rules.

## Definition

```c
static bool
is_valid_xml_namechar(pg_wchar c)
```
## Detailed Description
This function implements the XML specification's rules for valid characters that can appear anywhere in an XML name (except the first position). According to XML standards, XML name characters can include Letters (base or ideographic), Digits, specific punctuation marks (period, hyphen, underscore, colon), CombiningChar characters, and Extender characters. The function uses libxml2's character classification functions to check each category: xmlIsBaseCharQ() and xmlIsIdeographicQ() for letters, xmlIsDigitQ() for digits, xmlIsCombiningQ() for combining characters, and xmlIsExtenderQ() for extender characters. Additionally, it explicitly checks for the allowed punctuation characters.

This validation is essential for ensuring that SQL identifiers can be properly converted to valid XML names while preserving as much of the original identifier as possible.

## Parameters / Member Variables
- : A pg_wchar (Unicode codepoint) representing the character to be validated as a potential character in an XML name.

## Dependencies
- Functions called/Symbols referenced:
  - xmlIsBaseCharQ (libxml2 function to check if character is a base character)
  - xmlIsIdeographicQ (libxml2 function to check if character is ideographic)
  - xmlIsDigitQ (libxml2 function to check if character is a digit)
  - xmlIsCombiningQ (libxml2 function to check if character is combining)
  - xmlIsExtenderQ (libxml2 function to check if character is an extender)
- Called from (representative examples):
  - [map_sql_identifier_to_xml_name](../m/map_sql_identifier_to_xml_name.md)

## Notes and Other Information
- This is a static function, accessible only within the xml.c compilation unit
- The function implements the XML 1.0 specification's NameChar production rule
- The comment "Letter | Digit | '.' | '-' | '_' | ':' | CombiningChar | Extender" references the XML specification grammar
- More permissive than is_valid_xml_namefirst, allowing digits and additional character types
- Combining characters include diacritical marks and other characters that modify base characters
- Extender characters include characters like the middle dot (·) used in some languages
- The colon character has special meaning in XML namespaces but is allowed in names
- This function works in conjunction with is_valid_xml_namefirst to validate complete XML names
- Returns true if the character can be used anywhere in an XML name, false otherwise

## Simplified Source

```c
static bool
is_valid_xml_namechar(pg_wchar c)
{
    // Check if character is valid anywhere in XML name
    // XML NameChar ::= Letter | Digit | '.' | '-' | '_' | ':' | CombiningChar | Extender
    return (xmlIsBaseCharQ(c) ||      // Base letters
            xmlIsIdeographicQ(c) ||   // Ideographic characters
            xmlIsDigitQ(c) ||         // Digits (0-9)
            c == '.' || c == '-' ||   // Period and hyphen
            c == '_' || c == ':' ||   // Underscore and colon
            xmlIsCombiningQ(c) ||     // Combining characters (diacritics)
            xmlIsExtenderQ(c));       // Extender characters
}
```