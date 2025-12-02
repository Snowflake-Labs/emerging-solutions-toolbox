# Stellar BI - Solution Engineer Quick Start Guide

## What is Stellar BI?

Stellar BI parses Power BI files (`.pbit` and `.bim`) and provides two main capabilities:

1. **Generate Semantic Views** - Creates Snowflake semantic views for AI applications (Cortex Analyst), cross-platform BI tools (Tableau, Looker), and centralized business logic.
2. **Performance Optimization** - Analyzes Power Query transformations and partition logic to optimize Power BI performance by moving heavy transformations to Snowflake.
---

## Requirements
- Only Snowflake sourced tables are supported.  If tables are using other sources such as SQL server or Excel, data will first need to be migrated into Snowflake via SnowConvert, and Power BI files upodated to point to Snowflake.
- SSAS must use `.bim` files (not older XML-based versions)

---

## License

This project is licensed under the Apache 2.0 License - see [LICENSE](LICENSE) file for details.

---

## Qualifying Use Cases

| Use Case | Description |
|----------|-------------|
| **Semantic View for Snowflake Intelligence / Cortex Analyst** | Enable natural language querying over business metrics |
| **Multi-BI Tool Support** | Same business logic across multiple BI tools (Tableau, Sigma, custom dashboard) |
| **Power BI to Snowflake Migration** | Optimize BI layer during Snowflake migration |
| **Business Logic Centralization** | Single source of truth for calculations |
---

## Using Stellar BI

### Pre-Implementation Checklist
- [ ] Confirm all BI tables are sourced from Snowflake
- [ ] Obtain `.pbit` file (from Power BI) or `.bim` file (from SSAS)
- [ ] Big complex models defined in a single BI file should be broken down into multiple simpler single model files.

### Upload & Parse Model
1. Run [Stellar BI](#key-resources).
2. Upload `.pbit` or `.bim` file for parsing. Alternatively, you can use the test pbit file [test1.pbit](tests/data/test1.pbit) to try it out.

**After parsing, choose your use case:**

---

### Use Case 1: Generate Semantic View
Generate Snowflake semantic views for Cortex Analyst and Snowflake Intelligence.

#### Step 1: Configure Tables
- First provide a name to semantic view.
- Review primary key and table type (FACT or DIMENSION) for each table.
- Review any unsupported measures flaged by Stellar BI, they will need to be manually added.  

#### Step 2: Download SQLs
- Download two scripts:
   - `semantic_table_ddl.sql` (individual views for each table)
   - `semantic_view.sql` (main semantic model with relationships & measures)

#### Step 3: Review & Adjust
Expected conversion: ~80% auto-converted, ~20% requires manual review.
Use cursor to help with any tricky manual conversions.

**Common Manual Adjustments:**
- Unsupported DAX functions → Translate to SQL using window functions or subqueries
- Unsupported Calculated columns → Add to semantic table view
- Many-to-many relationships → Create bridge table

Review and validate generated SQL scripts using Cursor.

#### Step 4: Deliver to Customers
Provide SQL scripts (both files) to customers.

---

### Use Case 2: Performance Optimization

Analyze Power BI partition logic and move data transformations to Snowflake.

#### Step 1: Review Partitions
Review the partitions for each table:
- Current partition expressions and data sources.
- Power Query transformations for each partition.
- Import vs. DirectQuery mode for each partition.

#### Step 2: Identify Optimization Opportunities
Review each partition and look for:
- Data transformations that can be more efficiently performed in Snowflake instead of Power BI.
- Tables in Import mode that could use DirectQuery to Snowflake views, vice versa.
- Duplicate paritition and measure logic across different BI files that can be centralized in snowflake.

#### Step 3: Generate Updated BI File
1. Export updated `.bim` or `.pbit`
2. Export partition DDLs, review and validate DDLs with Cursor, manual adjustment maybe required for any unsupported PowerQuery functions.
3. Document any changes made in the bim or pbit file.
4. Provide customer partition DDLs and converted BI file with change history.

---

## Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| **Unsupported DAX Functions** | Manually translate to Snowflake SQL functions |
| **Unsupported Calculated columns** | Add to semantic table view definition |
| **Many-to-many relationships** | Create bridge table in Snowflake |
| **Non-Snowflake data sources** | Tables excluded—migrate data to Snowflake first |

---

## Key Resources
- **Stellar BI**: 
  Can be run in either:
  - Stellar BI in Snowhouse
  - Stellar BI installation in your demo account: see [Installation Instructions](README.md#method-b-install-stellar-bi-in-your-snowflake-account).  
- [**Walkthrough Video**](https://drive.google.com/file/d/14JZU3TSpNSpJH4KaSLqFDfmgThJIkEBW/view)
- **Docs**: 
  - [readme.md](README.md)
  - [limitations.md](limitations.md)
---
