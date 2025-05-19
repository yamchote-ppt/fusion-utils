# ETL Project

## Author
Phaphontee Yamchote

## Overview
This project provides a set of tools and utilities for managing ETL (Extract, Transform, Load) processes using PySpark in Microsoft Fabric Notebook. It includes modules for auditing, creating blank tables, and various utility functions to streamline data processing workflows.

## Installation
1. Download the `fusion.py` file and place it in your project directory (environment resource or built-in).

## Modules

### 1. `AuditLog`
This module is used for logging ETL activities and auditing data changes.

#### How to Use
```python
from env.fusion import AuditLog

ad = AuditLog(
   WS_ID='your_workspace_id',
   TABLE_NAME_to_check='table_to_check',
   AUDIT_TABLE_NAME='audit_table_name',
   LH_ID_to_check='lakehouse_id_to_check',
   LH_ID_audit='lakehouse_id_audit',
   schema='optional_schema'
)

ad.initialDetail(
   pipelineName='pipeline_name',
   pipelineId='pipeline_id',
   TriggerType='trigger_type',
   functionName='function_name'
)

ad.execute(
   ETL_func=your_function,
   raiseError=True
)
```

### 2. `CreateBlankTable`
This module helps create blank tables based on metadata files.

**Note:**  
In metatable CSV file, ensure that the CSV contains the following required columns:
- `TableName`: Name of the table to be created
- `LakehouseName`: Name of the lakehouse to save the table
- `ColumnName`: Name of the column
- `DataType`: Data type of the column (e.g., StringType, IntegerType, etc.)
- `Precision`: Precision for DecimalType (if applicable)
- `Scale`: Scale for DecimalType (if applicable)
- `forPartition`: 1 if the column is a partition column, 0 otherwise


#### How to Use
```python
from env.fusion import CreateBlankTable

cbt = CreateBlankTable(
   WS_ID='your_workspace_id',
   META_LH_ID='metadata_lakehouse_id',
   META_FILENAME='metadata_file_name',
   format='delta'  # or 'csv'
)

cbt.run()
```

### 3. `utils`
A collection of utility functions for common data processing tasks.

#### Examples
- **Trim String Columns**
  ```python
  from env.fusion import utils

  trimmed_df = utils.trim_string_columns(df)
  ```

- **Fill Null Values**
  ```python
  filled_df = utils.fillNaAll(df)
  ```

- **Track Table Size**
  ```python
  utils.trackSizeTable(df, detail='Table Details', schema=True, table=True)
  ```

## Notes
- Ensure that the required tables and metadata files exist before running the scripts.
- Use the `notebookutils` module for file system operations.

## License
This project is for internal use only for Fusion Solution. Unauthorized distribution is prohibited.