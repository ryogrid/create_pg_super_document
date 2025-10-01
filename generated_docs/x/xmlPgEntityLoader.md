# xmlPgEntityLoader

## Location
[src/backend/utils/adt/xml.c:2004-2021](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2004-L2021)

## Overview
A security-focused entity loader callback function that prevents loading of external XML entities by making them appear as empty strings.

## Definition

```c
struct */
	if (errcxt->magic != ERRCXT_MAGIC)
		elog(ERROR, "xml_ereport called with invalid PgXmlErrorContext");
```
## Detailed Description
xmlPgEntityLoader is a custom entity loader callback function designed to enhance XML processing security in PostgreSQL. Instead of allowing libxml2 to fetch external entities (which could pose security risks through XXE attacks or unwanted network access), this function silently prevents any external entity URL from being loaded by returning an empty string input stream.

This approach was chosen over throwing an error to avoid disrupting XML processing workflows while maintaining security. The function effectively neutralizes external entity references without causing parse failures.

## Parameters / Member Variables
- : The URL of the external entity to be loaded (ignored for security)
- : The public or system identifier of the entity (ignored for security)  
- : The XML parser context requesting the entity

## Dependencies
- Functions called/Symbols referenced:
  - xmlNewStringInputStream (libxml2 function)
- Called from (representative examples):
  - Used as callback in pg_xml_init function
  - Referenced in PgXmlErrorContext structure

## Notes and Other Information
- This function is part of PostgreSQL's defense against XML External Entity (XXE) attacks
- While it would be preferable to allow loading entities from the system's global XML catalog, the complexity and fragility of libxml2 APIs for this purpose led to the simpler approach of blocking all external access
- The function returns an xmlParserInputPtr that represents an empty string, effectively making external entities expand to nothing
- This is a static function only used within the xml.c module

## Simplified Source

```c
static xmlParserInputPtr
xmlPgEntityLoader(const char *URL, const char *ID,
                  xmlParserCtxtPtr ctxt)
{
    // Security: prevent loading external entities by returning empty string
    return xmlNewStringInputStream(ctxt, (const xmlChar *) "");
}
```