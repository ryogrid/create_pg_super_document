# XmlTableSetNamespace

## Location
src/backend/utils/adt/xml.c: 4789 - 4814

## Overview
Registers XML namespace declarations in the XPath context for use in XmlTable column expressions and row filters.

## Definition
static void XmlTableSetNamespace(TableFuncScanState *state, const char *name, const char *uri)

## Detailed Description
This function adds XML namespace declarations to the XPath evaluation context, allowing XmlTable expressions to reference XML elements and attributes using namespace prefixes. The function converts the namespace name and URI to libxml2-compatible strings and registers them with the XPath context using xmlXPathRegisterNs.

The function explicitly rejects DEFAULT namespace declarations (where name is NULL) as this feature is not supported in PostgreSQL's XmlTable implementation. This limitation ensures consistent behavior and avoids potential complications with default namespace handling.

## Parameters / Member Variables
- state: TableFuncScanState* - The table function scan state containing the XmlTable context
- name: const char* - The namespace prefix to register (cannot be NULL)
- uri: const char* - The namespace URI to associate with the prefix

## Dependencies
- Functions called/Symbols referenced:
  - [GetXmlTableBuilderPrivateData](../G/GetXmlTableBuilderPrivateData.md) (retrieves XmlTable context)
  - ereport (PostgreSQL error reporting for unsupported features)
  - pg_xmlCharStrndup (converts C strings to libxml2 format)
  - xmlXPathRegisterNs (libxml2 namespace registration)
  - xml_ereport (XML-specific error reporting)
- Called from (representative examples):
  - No direct callers found (likely called via table function interface)

## Notes and Other Information
- Only available when PostgreSQL is compiled with libxml2 support (USE_LIBXML)
- Explicitly rejects DEFAULT namespace declarations with a feature-not-supported error
- Essential for XPath expressions that need to reference namespaced XML elements
- Located in src/backend/utils/adt/xml.c:4789-4814
- Registered namespaces remain available throughout the XmlTable processing
- Part of the XmlTable setup phase before row processing begins