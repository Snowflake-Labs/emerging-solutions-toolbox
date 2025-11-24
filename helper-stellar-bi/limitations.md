# Stellar BI Limitations
## Supported Power BI Connectivity Mode
Support Direct and Import Modes, all other modes (e.g. Composite, Mixed are not supported).

## DAX Support
* DAX functions that are context sensitive (e.g. ALL, ALLEXCEPT etc) are not supported.
* A subset of DAX functions are supported:
  - `ABS` - Returns the absolute value
  - `ALL` - Returns all rows in a table, ignoring filters
  - `BLANK` - Returns a blank value
  - `CALCULATE` - Evaluates an expression in a modified filter context
  - `COMBINEVALUES` - Concatenates values with a delimiter
  - `COUNT` - Counts the number of rows with non-blank values
  - `COUNTA` - Counts the number of non-blank values
  - `COUNTROWS` - Counts the number of rows in a table
  - `CURRENCY` - Converts a value to currency format
  - `DATE` - Returns a date value from year, month, and day
  - `DATEDIFF` - Returns the difference between two dates
  - `DATESBETWEEN` - Returns dates between a start and end date
  - `DAY` - Returns the day of the month (1-31)
  - `DISTINCT` - Returns distinct values from a column
  - `DISTINCTCOUNT` - Counts the number of distinct values
  - `DIVIDE` - Divides two numbers with safe divide-by-zero handling
  - `EOMONTH` - Returns the last day of the month
  - `FORMAT` - Formats a value using a format string
  - `HASONEVALUE` - Returns TRUE when a column has only one distinct value
  - `HOUR` - Returns the hour (0-23)
  - `IF` - Returns one value if a condition is TRUE, another if FALSE
  - `INT` - Rounds down to the nearest integer
  - `ISBLANK` - Returns TRUE if a value is blank
  - `ISFILTERED` - Returns TRUE if a column is filtered
  - `LEFT` - Returns characters from the start of a string
  - `LOOKUPVALUE` - Returns the value for the row that meets specified criteria
  - `MAX` - Returns the maximum value
  - `MIN` - Returns the minimum value
  - `MINUTE` - Returns the minute (0-59)
  - `MONTH` - Returns the month (1-12)
  - `PREVIOUSMONTH` - Returns dates from the previous month
  - `REPT` - Repeats text a specified number of times
  - `RIGHT` - Returns characters from the end of a string
  - `ROUND` - Rounds a number to a specified number of digits
  - `SECOND` - Returns the second (0-59)
  - `SELECTCOLUMNS` - Creates a table with selected columns
  - `SELECTEDVALUE` - Returns the value when a column has only one distinct value
  - `SUM` - Sums values in a column
  - `SUMX` - Evaluates an expression for each row and returns the sum
  - `TODAY` - Returns the current date
  - `UNICHAR` - Returns the Unicode character for a code point
  - `UNION` - Combines tables
  - `VALUES` - Returns distinct values from a column (respects filters)
  - `YEAR` - Returns the year


## Power Query M Support
* A subset of Power Query M functions are supported:
  - `Binary.Decompress` - Decompresses binary data
  - `Binary.FromText` - Converts text to binary
  - `Csv.Document` - Parses CSV data
  - `Date.EndOfMonth` - Returns the last day of the month
  - `Date.From` - Converts a value to a date
  - `Date.IsInCurrentYear` - Returns TRUE if date is in current year
  - `Excel.Workbook` - Reads Excel workbook data
  - `File.Contents` - Reads file contents
  - `Json.Document` - Parses JSON data
  - `List.Contains` - Returns TRUE if a list contains a value
  - `Snowflake.Databases` - Connects to Snowflake databases
  - `Table.AddColumn` - Adds a column to a table
  - `Table.AddIndexColumn` - Adds an index column
  - `Table.Distinct` - Returns distinct rows
  - `Table.ExpandTableColumn` - Expands a column containing tables
  - `Table.FirstN` - Returns the first N rows
  - `Table.FromRecords` - Creates a table from records
  - `Table.FromRows` - Creates a table from rows
  - `Table.Join` - Joins two tables
  - `Table.NestedJoin` - Creates a nested join
  - `Table.PromoteHeaders` - Promotes first row to column headers
  - `Table.RemoveColumns` - Removes columns from a table
  - `Table.RenameColumns` - Renames columns
  - `Table.ReplaceValue` - Replaces values in a table
  - `Table.SelectColumns` - Selects specific columns
  - `Table.SelectRows` - Filters rows
  - `Table.TransformColumnTypes` - Changes column types
  - `Text.Combine` - Concatenates text values
  - `Text.Contains` - Returns TRUE if text contains a substring
  - `Text.From` - Converts a value to text
  - `Text.Proper` - Converts text to proper case
  - `Text.Start` - Returns characters from the start of text
  - `Value.NativeQuery` - Executes a native query  

If Value.NativeQuery is already being used, Stellar BI currently **doesn't** provide additional support to convert the custom SQL to View / Semantic Table View DDLs.

## Semantic View Support
Only Snowflake sourced semantic tables are supported.

| Power BI Model Element | Definition | Translation Supported | Snowflake Semantic View Element |
|------------------------|------------|----------------------|--------------------------------|
| **Semantic Table** | Semantic table containing columns and measures | ✅ YES | Semantic Table, a separte Snowflake View DDL is also provided to generate snowflake view for each semantic table. Primary Keys are imputed from relationship definition.|
| **Column** | Individual data field within a table | ✅ YES |Dimension or fact depending on the table type selected in UI, or inferred by the tool.|
| **Relationship** | Connection between two tables | ✅ YES | Relationship, cross-filtering and many to many relationship not supported.|
| **Measure** | Calculated metric (DAX formula) | ✅ YES | Metric, only supported DAX functions will be translated.|
| **Term** | Alternate name or synonym for objects (from cultures.linguisticMetadata) | ✅ YES | `WITH SYNONYMS` clause in Semantic Table definition.|
