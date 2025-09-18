# descriptor_item_name

## Location
[src/interfaces/ecpg/preproc/descriptor.c:233-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/descriptor.c#L233-L274)

## Overview
Converts ECPG descriptor type enumeration values to their corresponding SQL standard string names for use in generated code and error messages.

## Definition


## Detailed Description
This is a static utility function that maps ECPG descriptor type enumeration constants to their standardized SQL string representations. It provides a centralized translation mechanism between internal ECPG type codes and the human-readable names used in SQL descriptor operations. The function covers all supported descriptor item types including data characteristics, length information, type metadata, and special indicators.

The function uses a comprehensive switch statement to handle all descriptor item types:
- Cardinality and count information
- Data and indicator references  
- DateTime interval codes and precision
- Length measurements (regular and returned)
- Name and type information
- Nullability and key member status
- Precision and scale for numeric types
- Octet length measurements

## Parameters / Member Variables
- : An ECPGdtype enumeration value representing a specific descriptor item type

## Dependencies
- Functions called/Symbols referenced:
  - enum ECPGdtype (enumeration type for descriptor items)
  - [ECPGd_cardinality](../E/ECPGd_cardinality.md), ECPGd_count, ECPGd_data (descriptor type constants)
  - ECPGd_di_code, ECPGd_di_precision (datetime interval constants)
  - ECPGd_indicator, ECPGd_key_member, ECPGd_length (descriptor metadata constants)
  - ECPGd_name, ECPGd_nullable, ECPGd_octet (name and nullability constants)
  - ECPGd_precision, ECPGd_ret_length, ECPGd_ret_octet (precision and return length constants)
  - ECPGd_scale, ECPGd_type (scale and type constants)
- Called from (representative examples):
  - [output_set_descr](../o/output_set_descr.md) at src/interfaces/ecpg/preproc/descriptor.c:292
  - [output_set_descr](../o/output_set_descr.md) at src/interfaces/ecpg/preproc/descriptor.c:302

## Notes and Other Information
- This is a static function, only accessible within the descriptor.c file
- Returns NULL for unrecognized or invalid descriptor item codes
- The string names follow SQL standard descriptor item naming conventions
- Used primarily in error messages and debug output to provide human-readable descriptor item names
- The function provides complete coverage of all ECPG descriptor types supported by the preprocessor
- String constants are uppercase following SQL naming conventions
- Essential for mapping between internal representation and external SQL standards