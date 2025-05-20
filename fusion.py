from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from typing import Union, List, Tuple, Any, Optional, Dict, Callable
import notebookutils
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

spark = SparkSession.builder.appName("fusion").getOrCreate()

class _AuditLog_Fusion:
    """
    Base class for audit logging of ETL processes into a Delta-based audit table.

    Attributes:
        WS_ID: Workspace identifier.
        TABLE_NAME_to_check: Source table name.
        AUDIT_TABLE_NAME: Audit table name.
        LH_ID_to_check: Lakehouse ID of source table.
        LH_ID_audit: Lakehouse ID for audit table.
        schema: Optional schema name.
        log: In-memory storage of audit fields.

    Usage:
    ------
    >>> cols = ['PIPELINENAME','PIPELINERUNID','TRIGGERTYPE','TABLE_NAME','FUNCTION_NAME',
                'COUNTROWSBEFORE','COUNTROWSAFTER','ERRORCODE','ERRORMESSAGE']
    >>> logger = _AuditLog_Fusion(cols, WS_ID='ws', TABLE_NAME_to_check='src',
                                 AUDIT_TABLE_NAME='audit', LH_ID_to_check='lh')
    """
    class logger:
        def __init__(self, **kwargs):
            self._data = kwargs
        def __getitem__(self, key): return self._data[key]
        def __setitem__(self, key, value):
            if key not in self._data:
                raise KeyError(f"Cannot add new key: {key}")
            self._data[key] = value
        def __repr__(self): return repr(self._data)

    def __init__(
        self,
        columns: Union[List[str], Tuple[str, ...]],
        WS_ID: str,
        TABLE_NAME_to_check: str,
        AUDIT_TABLE_NAME: str,
        LH_ID_to_check: str,
        LH_ID_audit: Optional[str] = None,
        schema: Optional[str] = None
    ) -> None:
        """
        Initialize the audit logger.

        Parameters:
            columns: Audit field names.
            WS_ID: Workspace ID.
            TABLE_NAME_to_check: Source table.
            AUDIT_TABLE_NAME: Audit table.
            LH_ID_to_check: Source lakehouse ID.
            LH_ID_audit: Audit lakehouse ID.
            schema: Optional schema within lakehouse.
        """
        self.WS_ID = WS_ID
        self.TABLE_NAME_to_check = TABLE_NAME_to_check
        self.AUDIT_TABLE_NAME = AUDIT_TABLE_NAME
        self.LH_ID_to_check = LH_ID_to_check
        self.LH_ID_audit = LH_ID_audit or LH_ID_to_check
        self.schema = schema
        self.fixColumns = {'STARTTIME','ENDTIME','AUDITKEY','STATUS_ACTIVITY'}
        self.columns = tuple(set(columns).union(self.fixColumns))

        base_uri = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/"
        if schema:
            self.PATH_TO_CHECKED_TABLE = f"{base_uri}{LH_ID_to_check}/Tables/{schema}/{TABLE_NAME_to_check}"
            self.PATH_TO_AUDIT_TABLE = f"{base_uri}{LH_ID_audit}/Tables/{schema}/{AUDIT_TABLE_NAME}"
        else:
            self.PATH_TO_CHECKED_TABLE = f"{base_uri}{LH_ID_to_check}/Tables/{TABLE_NAME_to_check}"
            self.PATH_TO_AUDIT_TABLE = f"{base_uri}{LH_ID_audit}/Tables/{AUDIT_TABLE_NAME}"

        self.log = self.logger(**{col: None for col in self.columns})
        self.log['STATUS_ACTIVITY'] = 'Not start'

    def setKeys(self, initConfig: Dict[str, Any]) -> None:
        """
        Bulk set audit fields.

        Usage:
        >>> logger.setKeys({'PIPELINENAME':'pipe','TRIGGERTYPE':'manual'})
        """
        assert set(initConfig).issubset(self.columns), f"Keys must be subset of {self.columns}"
        for k,v in initConfig.items(): self.log[k] = v

    def setKey(self, key: str, value: Any) -> None:
        """
        Set single audit field.

        Usage:
        >>> logger.setKey('STATUS_ACTIVITY','Running')
        """
        assert key in self.columns, f"Key must be in {self.columns}"
        self.log[key] = value

    def initialDetail(self, initConfig: Dict[str, Any]) -> None:
        """Initialize log fields."""
        self.setKeys(initConfig)

    def startAudit(self) -> None:
        """Mark audit start time and status."""
        ts = datetime.now() + timedelta(hours=7)
        self.log['STARTTIME'] = ts.isoformat()
        self.log['STATUS_ACTIVITY'] = 'logging ...'

    def countBefore(self) -> None:
        """Log row count before ETL."""
        if self.log['COUNTROWSBEFORE']:
            raise ValueError('COUNTROWSBEFORE already set')
        self.log['COUNTROWSBEFORE'] = spark.read.load(self.PATH_TO_CHECKED_TABLE).count()

    def countAfter(self) -> None:
        """Log row count after ETL."""
        self.log['COUNTROWSAFTER'] = spark.read.load(self.PATH_TO_CHECKED_TABLE).count()

    def _endAuditLog(self) -> DataFrame:
        """Persist log to audit table."""
        self.log['ENDTIME'] = (datetime.now()+timedelta(hours=7)).isoformat()
        row = {k:[str(self.log[k])] for k in self.log}
        df = spark.createDataFrame(list(zip(*row.values())), schema=list(row.keys()))
        df.write.mode('append').save(self.PATH_TO_AUDIT_TABLE)
        return df

    def endSuccess(self) -> None:
        """Mark success and persist log."""
        self.log['STATUS_ACTIVITY'] = 'Success'
        self._endAuditLog()
        print(self)

    def endFail(self, errorCode:str, errorMessage:Any) -> None:
        """Mark failure, record error, and persist log."""
        self.log['STATUS_ACTIVITY'] = 'Fail'
        self.log['ERRORCODE'] = errorCode
        self.log['ERRORMESSAGE'] = str(errorMessage)
        self._endAuditLog()
        print(self)

    def getAuditLogTable(self) -> DataFrame:
        """Retrieve full audit log as DataFrame."""
        return spark.read.load(self.PATH_TO_AUDIT_TABLE)

    def getAllPath(self) -> Dict[str,str]:
        """Get paths for checked and audit tables."""
        return {'checked':self.PATH_TO_CHECKED_TABLE,'audit':self.PATH_TO_AUDIT_TABLE}

    def __repr__(self) -> str:
        return repr(self.log)
    
class AuditLog(_AuditLog_Fusion):
    
    def __init__(self, WS_ID: str, TABLE_NAME_to_check:str, AUDIT_TABLE_NAME:str, LH_ID_to_check: str, LH_ID_audit: str = None, schema: str = None):
        '''
        - if `LH_ID_audit` is not given, it is  LH_ID_to_check automatically, i.e. audit table is in the same lakehouse as that of
        - if using lakehouse with Schema, please provide `schema` parameter

        How to use:
        -----------
        ```
        from env.fusion import AuditLog

        ad = utils.AuditLog(
            WS_ID = 'your workspace id',
            TABLE_NAME_to_check = 'the table you want to check',
            AUDIT_TABLE_NAME = 'your audit table name',
            LH_ID_to_check = 'your lakehouse id the cheched table is in',
            LH_ID_audit = 'your lakehouse id the audit table is in',
            )
        ad.initialDetail( 
            pipelineName = 'running pipeline name', 
            pipelineId = 'pipeline id',
            TriggerType = 'trigger type',
            functionName = 'name of the function you want to run'
        )

        ad.execute( # this will run your function and log the result
            ETL_func = [function you want to run without parenthesis],
            raiseError = True # if you want to raise error when the function fail, default is True
        )
        ```
        '''
        super().__init__(['PIPELINENAME', 'PIPELINERUNID', 'TRIGGERTYPE', 'TABLE_NAME', 'FUNCTION_NAME','COUNTROWSBEFORE', 'COUNTROWSAFTER', 'ERRORCODE', 'ERRORMESSAGE'] ,WS_ID, TABLE_NAME_to_check, AUDIT_TABLE_NAME, LH_ID_to_check, LH_ID_audit, schema)

    def initialDetail(
        self,
        pipelineName: str,
        pipelineId: str,
        TriggerType: str,
        functionName: str
    ) -> None:
        """
        Initialize and start audit for an ETL run.

        Args:
            pipelineName (str): A human-readable name for the ETL pipeline.
            pipelineId (str): A unique identifier for this pipeline run.
            TriggerType (str): How the pipeline was triggered (e.g., 'manual', 'scheduled').
            functionName (str): The name of the Python function executing ETL logic.

        Returns:
            None

        After calling this method, the audit log will have:
          - STARTTIME set to current timestamp (+7h offset)
          - STATUS_ACTIVITY set to 'logging ...'
          - PIPELINENAME, PIPELINERUNID, TRIGGERTYPE, TABLE_NAME, FUNCTION_NAME
          - AUDITKEY composed of `<pipelineName>-<TABLE_NAME>-<STARTTIME>`

        Example:
            >>> ad = AuditLog('ws1', 'customers', 'audit_customers', 'lh_cust', 'lh_audit')
            >>> ad.initialDetail('DailyLoad', 'run123', 'scheduled', 'load_customers')
            >>> print(ad.log)
        """
        super().initialDetail({
            'PIPELINENAME': pipelineName,
            'PIPELINERUNID': pipelineId,
            'TRIGGERTYPE': TriggerType,
            'TABLE_NAME': self.TABLE_NAME_to_check,
            'FUNCTION_NAME': functionName
        })
        # start the audit clock
        self.startAudit()
        # generate a unique key for this run
        self.log['AUDITKEY'] = (
            f"{pipelineName}-{self.TABLE_NAME_to_check}-"
            f"{self.log['STARTTIME'].replace(':','_')}"
        )

    def execute(
        self,
        ETL_func: Callable[[], Any],
        raiseError: bool = True
    ) -> bool:
        """
        Execute the ETL function within the audit context.

        Args:
            ETL_func (Callable[[], Any]): A parameterless function encapsulating the ETL logic.
            raiseError (bool): If True, re-raises any exception after logging failure. Defaults to True.

        Returns:
            bool: True if ETL_func completes successfully; False if an exception occurs and raiseError=False.

        Behavior:
          1. Calls countBefore() to log pre-ETL row count.
          2. Executes the provided ETL_func().
          3. Calls countAfter() to log post-ETL row count.
          4. On success, calls endSuccess() to mark audit log as SUCCESS.
          5. On exception, calls endFail() to mark audit log as FAIL and record error.

        Example:
            >>> def sample_etl():
            ...     # ETL steps here
            ...     pass
            >>> success = ad.execute(sample_etl)
            >>> if not success:
            ...     print("ETL failed, but continuing...")
        """
        try:
            # log rows before
            self.countBefore()
            # run ETL logic
            ETL_func()
            # log rows after
            self.countAfter()
            # mark success
            self.endSuccess()
            return True
        except Exception as e:
            # log failure details
            self.endFail('-', e)
            if raiseError:
                raise
            return False

class CreateBlankTable:
    def __init__(self, WS_ID, META_LH_ID, META_FILENAME, format="delta", optionalMapper:dict[str, DataType] = None):
        '''
        For creating a blank table in lakehouse from a metadata file.
        METATABLE must have the following columns:
        - TableName: Name of the table to be created
        - LakehouseName: Name of the lakehouse to save the table
        - ColumnName: Name of the column
        - DataType: Data type of the column (e.g., StringType, IntegerType, etc.)
        - Precision: Precision for DecimalType (if applicable)
        - Scale: Scale for DecimalType (if applicable)
        - forPartition: 1 if the column is a partition column, 0 otherwise

        How to use:
        -----------
        ```
        from env.fusion import CreateBlankTable
        cbt = CreateBlankTable(
            WS_ID = 'your workspace id',
            META_LH_ID = 'your lakehouse id the metadata file is in',
            META_FILENAME = 'your metadata file name',
            format = 'delta' # or 'csv'
        )
        cbt.run() # this will create all tables in the metadata file
        ```
        '''
        self.WS_ID = WS_ID
        self.META_LH_ID = META_LH_ID
        self.META_FILENAME = META_FILENAME
        assert format in ["delta", "csv"], "format should be either 'delta' or 'csv'"
        self.format = format
        if self.format == 'csv':
            self.META_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.META_LH_ID}/Files/{self.META_FILENAME}'
        elif self.format == 'delta':
            self.META_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.META_LH_ID}/Tables/{self.META_FILENAME}'

        self.MAPPER = {
            'stringtype': StringType,
            'integertype': IntegerType,
            'longtype': LongType,
            'floattype': FloatType,
            'doubletype': DoubleType,
            'booleantype': BooleanType,
            'Datetype': DateType,
            'timestamptype': TimestampType,
            'decimaltype': DecimalType,
            'binarytype': BinaryType,
            'str': StringType,
            'int': IntegerType,
            'long': LongType,
            'float': DecimalType,
            'double': DecimalType,
            'boolean': BooleanType,
            'decimal': DecimalType,
            'short': ShortType,
        }
        if optionalMapper:
            for key, value in optionalMapper.items():
                if key not in self.MAPPER:
                    self.MAPPER[key] = value
                else:
                    raise KeyError(f'{key} already exist in the mapper')
    
    def create_table(self, table_name:str, lh_name:str, column_datatype_map:Dict[str,DataType],
                     precision_scale_map:Dict[str,Tuple[int,int]], table_partition_columns:Optional[List[str]]=None) -> None:
        """
        Create an empty Delta table in the lakehouse with the given schema.

        Args:
            table_name (str): Name of the table to create.
            lh_name (str): Display name of the target lakehouse.
            column_datatype_map (dict): Keys are column names, values are DataType classes
                (e.g., StringType, IntegerType).
            precision_scale_map (dict): For DecimalType columns, a mapping of column name
                to (precision, scale). Other types may be omitted.
            table_partition_columns (list, optional): Columns to partition by. Default None.

        Raises:
            KeyError: If `lh_name` cannot be resolved to a Lakehouse ID.
            AnalysisException: If table creation fails.

        Example:
        -------
            >>> column_map = {'id': IntegerType, 'amount': DecimalType}
            >>> precision_map = {'amount': (10, 2)}
            >>> cbt.create_table(
            ...     table_name='transactions',
            ...     lh_name='finance_lh',
            ...     column_datatype_map=column_map,
            ...     precision_scale_map=precision_map,
            ...     table_partition_columns=['year']
            ... )
        """
        LH_ID = utils.get_lh_id(self.WS_ID, lh_name)
        table_path = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Tables/{table_name}'

        # Create schema for the table
        schema_fields = []
        for column, datatype in column_datatype_map.items():
            precision, scale = precision_scale_map.get(column, (None, None))
            if datatype == DecimalType: 
                if precision is None:
                    precision = 38
                if scale is None:
                    scale = 18
                schema_fields.append(StructField(column, datatype(precision, scale), True))
            else:
                schema_fields.append(StructField(column, datatype(), True))
        schema = StructType(schema_fields)

        empty_df = spark.createDataFrame([], schema)

        writer = empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if table_partition_columns:
            writer = writer.partitionBy(*table_partition_columns)
        writer.save(table_path)

    def readMetaFile(self) -> pd.DataFrame:
        """
        Read metadata definitions into a Pandas DataFrame.

        Returns:
            pandas.DataFrame: Metadata indexed by ('TableName', 'LakehouseName'), containing columns
            ['TableName', 'LakehouseName', 'ColumnName', 'DataType', 'Precision', 'Scale', 'forPartition'].

        Raises:
            AnalysisException: If the metadata path cannot be read.

        Usage:
        ------
        >>> cbt = CreateBlankTable('ws1', 'meta_lh', 'meta.csv', format='csv')
        >>> meta_df = cbt.readMetaFile()
        >>> print(meta_df.head())
        """
        if  self.format == 'csv':
            meta_df = spark.read.option("header", "true").option("delimiter", ",").csv(self.META_PATH).toPandas().set_index(['TableName','LakehouseName'], drop=False)
            return meta_df
        elif self.format == 'delta':
            meta_df = spark.read.format("delta").load(self.META_PATH).toPandas().set_index(['TableName','LakehouseName'], drop=False)
            return meta_df

    def create_all(self, meta_table:pd.DataFrame) -> None:
        """
        Iterate over metadata and create one table per entry.

        Args:
            meta_table (pandas.DataFrame): Metadata indexed by ('TableName', 'LakehouseName').

        Returns:
            None

        Usage:
        ------
            >>> cbt = CreateBlankTable('ws1', 'meta_lh', 'meta.csv')
            >>> meta = cbt.readMetaFile()
            >>> cbt.create_all(meta)
        """
        # raise NotImplementedError("create_all method should be implemented in the subclass.")
        tableNames = meta_table.index.get_level_values(0).unique()
        for table in tableNames:
            table_meta = meta_table.loc[table]
            lakehouse_name = table_meta.index.unique()[0]
            column_datatype_map = {}
            precision_scale_map = {}
            table_partition_columns = []
            for index, row in table_meta.iterrows():
                column_name = row['ColumnName']
                datatype_str = row['DataType']
                datatype = self.MAPPER[datatype_str]

                column_datatype_map[column_name] = datatype
                if datatype == DecimalType:
    
                    # coerce back to int, with defaults if missing
                    raw_p = row['Precision']
                    raw_s = row['Scale']
                    try:
                        p = int(raw_p)
                    except (TypeError, ValueError):
                        p = 38
                    try:
                        s = int(raw_s)
                    except (TypeError, ValueError):
                        s = 18
                    precision_scale_map[column_name] = (p, s)
                if row['forPartition'] == 1:
                    table_partition_columns.append(column_name)
            self.create_table(table, lakehouse_name, column_datatype_map, precision_scale_map, table_partition_columns)

    def run(self) -> None:
        """
        Execute the end-to-end creation of all tables defined in metadata.

        Steps:
          1. readMetaFile() to load metadata into a DataFrame.
          2. create_all() to create each table as defined.

        Usage:
        ------
            >>> cbt = CreateBlankTable('ws1', 'meta_lh', 'meta.csv')
            >>> cbt.run()
        """
        self.meta_table = self.readMetaFile()
        self.create_all(self.meta_table)

class utils:
    """Utility functions for DataFrame and lakehouse operations."""

    @staticmethod
    def trim_string_columns(df: DataFrame) -> DataFrame:
        """
        Trim whitespace from all string-typed columns in the DataFrame.

        Args:
            df (DataFrame): The input Spark DataFrame containing string columns.

        Returns:
            DataFrame: A new DataFrame where leading and trailing whitespace has been removed
                       from every string-typed column.

        Example:
            >>> from pyspark.sql import SparkSession
            >>> spark = SparkSession.builder.getOrCreate()
            >>> df = spark.createDataFrame([(" Alice ", " Bob ")], ["name", "friend"])
            >>> utils.trim_string_columns(df).show()
            +-----+------+
            | name|friend|
            +-----+------+
            |Alice|   Bob|
            +-----+------+
        """
        string_columns = [f.name for f in df.schema.fields if f.dataType.typeName() == 'string']
        for col_name in string_columns:
            df = df.withColumn(col_name, trim(col(col_name)))
        return df

    @staticmethod
    def fillNaAll(df: DataFrame) -> DataFrame:
        """
        Fill all null values in the DataFrame with type-specific defaults.

        Args:
            df (DataFrame): The input Spark DataFrame whose nulls are to be replaced.

        Returns:
            DataFrame: A new DataFrame with nulls replaced by defaults for each column type.

        Raises:
            TypeError: If a column’s data type has no defined default fill value.

        Default fill values:
            - StringType: ''
            - ShortType: 0
            - IntegerType: 0
            - LongType: 0
            - FloatType: 0.0
            - DoubleType: 0.0
            - TimestampType: '1970-01-01 00:00:00'
            - DecimalType: 0.0

        Example:
            >>> df = spark.createDataFrame([(None, 1, None)], ["name", "age", "balance"])
            >>> utils.fillNaAll(df).show()
            +----+---+-------+
            |name|age|balance|
            +----+---+-------+
            |    |  1|    0.0|
            +----+---+-------+
        """
        fillnaDefault = {
            StringType: '',
            ShortType: 0,
            IntegerType: 0,
            LongType: 0,
            FloatType: 0.0,
            DoubleType: 0.0,
            TimestampType: '1970-01-01 00:00:00',
            DecimalType: 0.0,
        }
        fill_values = {}
        for field in df.schema.fields:
            field_type = type(field.dataType)
            if field_type in fillnaDefault:
                fill_values[field.name] = fillnaDefault[field_type]
            else:
                raise TypeError(f'No fill value defined for type {field_type}')
        return df.fillna(fill_values)

    @staticmethod
    def fillNaAllStringType(df: DataFrame, value: str = 'NA') -> DataFrame:
        """
        Fills all string-typed columns in the DataFrame with a given value where null.

        Args:
            df (DataFrame): The input Spark DataFrame.
            value (str): Replacement for null string values. Default is 'NA'.

        Returns:
            DataFrame: A new DataFrame where all string columns have been filled.

        Example:
            >>> df = spark.createDataFrame([(None, "Bob"), ("Alice", None)], ["name", "friend"])
            >>> utils.fillNaAllStringType(df, 'Unknown').show()
            +-------+-------+
            |   name| friend|
            +-------+-------+
            |Unknown|    Bob|
            |  Alice|Unknown|
            +-------+-------+
        """
        fillnaDefault = { StringType: value }
        fill_values = {}
        for field in df.schema.fields:
            if type(field.dataType) in fillnaDefault:
                fill_values[field.name] = fillnaDefault[type(field.dataType)]
        return df.fillna(fill_values)

    @staticmethod
    def copySchemaByName(df: DataFrame, fromDf: DataFrame) -> DataFrame:
        """
        Cast columns in `df` to match the data types of `fromDf` by column name.

        Args:
            df (DataFrame): The DataFrame to cast.
            fromDf (DataFrame): Reference DataFrame whose schema provides target types.

        Returns:
            DataFrame: A new DataFrame `df` with columns cast to the types found in `fromDf`.

        Example:
            >>> template = spark.createDataFrame([(1,)], ['value']).withColumn('value', col('value').cast('double'))
            >>> df = spark.createDataFrame([(1,)], ['value'])
            >>> utils.copySchemaByName(df, template).printSchema()
            root
             |-- value: double (nullable = true)
        """
        for field in fromDf.schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        return df

    @staticmethod
    def getSetColumn(df: DataFrame, columnName: str) -> set:
        """
        Return the unique values of a column as a Python set.

        Args:
            df (DataFrame): The input DataFrame.
            columnName (str): Name of the column to extract uniques from.

        Returns:
            set: Unique values from the specified column.

        Example:
            >>> df = spark.createDataFrame([(1,), (2,), (1,)], ['id'])
            >>> utils.getSetColumn(df, 'id')
            {1, 2}
        """
        return set(df.select(columnName).toPandas()[columnName])

    @staticmethod
    def getCountColumn(df: DataFrame, columnName: str) -> pd.Series:
        """
        Count occurrences of each distinct value in a column.

        Args:
            df (DataFrame): The input DataFrame.
            columnName (str): Name of the column to analyze.

        Returns:
            pandas.Series: Counts of each unique value in `columnName`.

        Example:
            >>> df = spark.createDataFrame([('A',), ('B',), ('A',)], ['cat'])
            >>> utils.getCountColumn(df, 'cat')
            A    2
            B    1
            dtype: int64
        """
        pandas_df = df.select(columnName).toPandas()
        return pandas_df[columnName].value_counts()

    @staticmethod
    def trackSizeTable(df: DataFrame, detail: Optional[str] = None, schema: bool = False, table: bool = False) -> None:
        """
        Print the row and column counts of the DataFrame, with optional schema and sample rows.

        Args:
            df (DataFrame): The DataFrame to inspect.
            detail (str, optional): Label to print before size info.
            schema (bool): If True, also print the DataFrame schema.
            table (bool): If True, display the first 5 rows.

        Returns:
            None

        Example:
            >>> utils.trackSizeTable(df, detail='Before load', schema=True, table=True)
        """
        if detail:
            print(f"{detail}: size = ", end='')
        numrow = df.count()
        print(f"({numrow}, {len(df.columns)})")
        if schema:
            df.printSchema()
        if table:
            df.show(5)

    @staticmethod
    def trackSizeOnLake(tablePath: str) -> None:
        """
        Print the row and column count of a Spark table via SQL.

        Args:
            tablePath (str): Fully qualified Spark table name or path.

        Returns:
            None

        Example:
            >>> utils.trackSizeOnLake('database.schema.table')
        """
        numRow = spark.sql(f"SELECT COUNT(*) FROM {tablePath}").collect()[0][0]
        numCol = spark.sql(f"DESCRIBE {tablePath}").count()
        print(f"{tablePath}: size = ({numRow}, {numCol})")

    @staticmethod
    def getSizeOnLake(tablePath: str) -> Tuple[int, int]:
        """
        Retrieve the row and column counts of a Spark table via SQL.

        Args:
            tablePath (str): Fully qualified Spark table name or path.

        Returns:
            Tuple[int, int]: (number of rows, number of columns)

        Example:
            >>> rows, cols = utils.getSizeOnLake('db.schema.tbl')
        """
        numRow = spark.sql(f"SELECT COUNT(*) FROM {tablePath}").collect()[0][0]
        numCol = spark.sql(f"DESCRIBE {tablePath}").count()
        return numRow, numCol
    
    @staticmethod
    def scdType2(sourceTable, targetTable, primarykey, comparedColumns=None, sortTimeColumn = 'TimeStamp', startDate= 'startDate', endDate='endDate', activeFlag='activeFlag') -> DataFrame:
        """
        Implements Slowly Changing Dimension (SCD) Type 2 logic for tracking historical changes in a target table.
        This function compares a source table with a target table to identify new, updated, and unchanged records.
        It maintains historical data by marking old records as inactive and inserting new or updated records with
        appropriate start and end dates, as well as an active flag.
        Parameters:
        -----------
        sourceTable : pyspark.sql.DataFrame
            The source table containing the latest data.
        targetTable : pyspark.sql.DataFrame
            The target table containing historical data.
        primarykey : list of str
            The list of primary key columns used to uniquely identify records.
        comparedColumns : list of str or None
            The list of columns to compare between the source and target tables to detect changes.
            if None, use all columns not related to scdtype 2 tracking
        sortTimeColumn : str
            The column to track the latest record when primary key is duplicated
        startDate : str, optional
            The column name for the start date of a record's validity. Default is 'startDate'.
        endDate : str, optional
            The column name for the end date of a record's validity. Default is 'endDate'.
        activeFlag : str, optional
            The column name for the active flag indicating whether a record is currently active. Default is 'activeFlag'.
        Returns:
        --------
        pyspark.sql.DataFrame
            A DataFrame containing the updated target table with SCD Type 2 logic applied. It includes:
            - New records from the source table.
            - Updated records with old versions marked as inactive and new versions added.
            - Unchanged records retained as-is.
            - Duplicate records from the source table handled appropriately.
        Notes:
        ------
        - The function assumes that the target table already contains the `startDate`, `endDate`, and `activeFlag` columns.
        - The `TimeStamp` column in the source table is used to determine the most recent record in case of duplicates.
        - The function uses a 7-hour offset for timestamp adjustments to align with a specific timezone.

        Example
        -------
        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.functions import current_timestamp
        >>> spark = SparkSession.builder.getOrCreate()
        >>> source = spark.createDataFrame([
        ...     (1, "A", "2024-06-01 10:00:00"),
        ...     (2, "B", "2024-06-01 11:00:00"),
        ... ], ["id", "value", "TimeStamp"])
        >>> target = spark.createDataFrame([
        ...     (1, "A", "2024-05-01", "9999-12-31","2024-03-01 10:00:00",  True),
        ...     (2, "C", "2024-05-01", "9999-12-31","2024-03-01 11:00:00", True),
        ... ], ["id", "value", "startDate", "endDate", "TimeStamp", "activeFlag"])
        >>> result = scdType2(
        ...     sourceTable=source,
        ...     targetTable=target,
        ...     primarykey=["id"],
        ...     comparedColumns=["value"],
        ...     sortTimeColumn="TimeStamp",
        ...     startDate="startDate",
        ...     endDate="endDate",
        ...     activeFlag="activeFlag"
        ... )
        """
        if comparedColumns is None:
            comparedColumns = [column for column in targetTable.columns if column not in primarykey + ['TimeStamp', 'AuditTimestamp', 'startDate', 'endDate', 'activeFlag','ModifiedDate']]

        sourceTable = sourceTable.drop_duplicates()

        window_spec = Window.partitionBy(primarykey).orderBy(col(sortTimeColumn).desc())
        sourceTable = sourceTable.withColumn("RowNum", row_number().over(window_spec))
        dup_sourceTable = sourceTable.filter(col("RowNum")>1).cache()
        dup_sourceTable = dup_sourceTable.withColumns({startDate:to_date(current_timestamp() + expr('INTERVAL 7 HOURS')), endDate:to_date(current_timestamp() + expr('INTERVAL 7 HOURS')), activeFlag: lit(False)}).drop('RowNum')

        sourceTable = sourceTable.filter(col("RowNum")==1).drop('RowNum')

        inactiveTarget = targetTable.filter(col(activeFlag) == False)
        activeTarget = targetTable.filter(col(activeFlag) == True)

        newRecords = sourceTable.join(activeTarget, on=primarykey, how='left_anti').withColumn(startDate, current_date() + expr('INTERVAL 7 HOURS')).withColumn(endDate, to_date(lit('9999-12-31'))).withColumn(activeFlag, lit(True)) #new PK
        expiredRecords = targetTable.join(sourceTable, on=primarykey, how='left_anti').withColumn(endDate, current_date() + expr('INTERVAL 7 HOURS')).withColumn(activeFlag, lit(False)) # should not have but I'm not sure

        sourceTable = sourceTable.withColumnsRenamed({column: f'{column}_source' for column in sourceTable.columns if column not in primarykey + [startDate, endDate, activeFlag]}) # rename only the columns which are compared

        commonRecords = sourceTable.join(activeTarget, on=primarykey, how='inner')

        compareConditions = [col(column) == col(column + '_source') for column in comparedColumns]
        compareCondition = compareConditions[0]
        for c in compareConditions[1:]:
            compareCondition = compareCondition & c
        sameRecord = commonRecords.filter(compareCondition).drop(*[column for column in commonRecords.columns if '_source' in column])

        changeRecordNew = commonRecords.filter(~compareCondition).select(primarykey + [column for column in commonRecords.columns if '_source' in column])\
            .withColumnsRenamed({column:column.replace('_source','') for column in commonRecords.columns})\
            .withColumns({startDate:to_date(current_timestamp() + expr('INTERVAL 7 HOURS')), endDate:to_date(lit('12/31/9999'), "MM/dd/yyyy"), activeFlag: lit(True)})
        changeRecordOld = commonRecords.filter(~compareCondition).select([column for column in commonRecords.columns if '_source' not in column])\
            .withColumns({endDate:to_date(current_timestamp() + expr('INTERVAL 7 HOURS')), activeFlag: lit(False)})

        final = dup_sourceTable.unionByName(inactiveTarget).unionByName(newRecords).unionByName(expiredRecords).unionByName(sameRecord).unionByName(changeRecordOld).unionByName(changeRecordNew)
        return final
    
    @staticmethod
    def get_ws_id() -> str:
        """
        Retrieve the current Azure Fabric workspace ID from Spark configuration.

        Returns:
            str: The workspace ID string.

        Example:
            >>> ws_id = utils.get_ws_id()
        """
        return spark.conf.get('trident.workspace.id')

    @staticmethod
    def get_lh_id(WS_ID: str, lh_name: str, caseSensitive: bool = True) -> str:
        """
        Resolve a lakehouse name to its ID within a workspace.

        Args:
            WS_ID (str): The Fabric workspace ID.
            lh_name (str): The display name of the lakehouse.
            caseSensitive (bool): Whether to match name with case sensitivity.

        Returns:
            str: The lakehouse ID.

        Raises:
            KeyError: If no matching lakehouse is found.

        Example:
            >>> lh_id = utils.get_lh_id('ws1', 'analytics_lh')
        """
        if caseSensitive:
            return notebookutils.lakehouse.get(lh_name, WS_ID)['id']
        for lh in notebookutils.lakehouse.list(WS_ID):
            if lh['displayName'].lower() == lh_name.lower():
                return lh['id']
        raise KeyError(f"Lakehouse '{lh_name}' not found")
            
class _UAT:
    def __init__(self, WS_ID, check_LH_ID, saveResult_LH_ID, saveResult_tableName, checklist_LH_ID, checklist_csvName):
        self.WS_ID = WS_ID
        self.check_LH_ID = check_LH_ID
        self.saveResult_LH_ID = saveResult_LH_ID
        self.saveResult_tableName = saveResult_tableName
        self.checklist_LH_ID = checklist_LH_ID
        self.checklist_csvName = checklist_csvName

        self.tablePath = f'abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{checklist_LH_ID}/Files/{checklist_csvName}'
        self.resultPath = f'abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{saveResult_LH_ID}/Tables/{saveResult_tableName}'
        checkList = pd.read_csv(self.tablePath, dtype={'idx':np.int32}).sort_values(by='idx').reset_index(drop=True)
        checkList = checkList[checkList['check']==1]
        checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey','additionalSQLFilter']] = checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey','additionalSQLFilter']].fillna('')
        self.checkList = checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey', 'additionalSQLFilter']]
        base_path = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.check_LH_ID}'
        data_types = ['Tables']
        
        if self.check_LH_ID != '':
            df = pd.concat([
                pd.DataFrame({
                    'tableName': [item.name.lower() for item in mssparkutils.fs.ls(f'{base_path}/{data_type}/')],
                    'type': data_type[:-1].lower() , 
                    'path': [item.path for item in mssparkutils.fs.ls(f'{base_path}/{data_type}/')],
        
                }) for data_type in data_types], ignore_index=True)
            df = df[['tableName', 'path']]
            self.pathToLoad = df.set_index('tableName')['path']
        else:
            self.pathToLoad = None
    
    def getCheckedTablePath(self, tableName):
        return self.pathToLoad.loc[tableName.lower()]

    def getChecklistTable(self):
        return self.checkList

    def get_file_table_list(self):
        return self.pathToLoad
    
class SQLgenerator(_UAT):
    """
    Generate SQL validation queries from a UAT checklist.

    This class reads a checklist of data quality checks and builds
    corresponding T-SQL statements (e.g. count, distinct, sum, etc.).
    It can then assemble them into a single UNION ALL script.

    Args:
        WS_ID (str): Fabric workspace ID.
        checklist_LH_ID (str): Lakehouse ID where the checklist CSV resides.
        checklist_csvName (str): Filename of the checklist CSV.
        saveQuery_fileName (str): Path (within the workspace) to save the SQL output.

    Attributes:
        checkList (pd.DataFrame): Filtered DataFrame with the columns
            ['idx','Table','Column','KeyCheck','groupbyKey','additionalSQLFilter'].
        sql (str or None): Cached UNION ALL SQL string once generated.

    Example:
        >>> gen = SQLgenerator(
        ...     WS_ID='ws1',
        ...     checklist_LH_ID='lh_checks',
        ...     checklist_csvName='checks.csv',
        ...     saveQuery_fileName='queries.sql'
        ... )
        >>> sql_text = gen.generateSQL()
        >>> print(gen.getSQL())
    """

    def __init__(
        self,
        WS_ID: str,
        checklist_LH_ID: str,
        checklist_csvName: str,
        saveQuery_fileName: str
    ) -> None:
        """
        Initialize the SQLgenerator with checklist location and output file.

        Args:
            WS_ID (str): Workspace identifier.
            checklist_LH_ID (str): Lakehouse ID of the checklist file.
            checklist_csvName (str): The checklist CSV file name.
            saveQuery_fileName (str): Where to save the generated SQL.
        """
        super().__init__(
            WS_ID=WS_ID,
            check_LH_ID='',
            saveResult_LH_ID='',
            saveResult_tableName='',
            checklist_LH_ID=checklist_LH_ID,
            checklist_csvName=checklist_csvName
        )
        # We only care about the check-defining columns
        self.checkList = self.checkList[
            ['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey', 'additionalSQLFilter']
        ]
        self.sql = None

    def countrowQuery(
        self,
        idx: int,
        Table: str,
        Column: str,
        KeyGroupby: str,
        additionalSQLFilter: Optional[str] = None
    ) -> str:
        """
        Build a COUNT(*) query for a table.

        Args:
            idx (int): Unique index of the check.
            Table (str): Database table name.
            Column (str): (unused) column parameter placeholder.
            KeyGroupby (str): (unused) groupby parameter placeholder.
            additionalSQLFilter (str, optional): WHERE clause fragment.

        Returns:
            str: A T-SQL SELECT statement counting rows.

        Example:
            >>> gen.countrowQuery(1, 'Customer', '', '', "status='A'")
            "SELECT 1 AS [index], 'customer' AS [Table], '' AS [Column], 'countrow' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(COUNT(*) AS FLOAT) AS [ValueOnPrem] FROM dbo.Customer WHERE status='A'"
        """
        where = f" WHERE {additionalSQLFilter}" if additionalSQLFilter else ""
        return (
            f"SELECT {idx} AS [index], "
            f"'{Table.lower()}' AS [Table], "
            f"'' AS [Column], "
            f"'countrow' AS [KeyCheck], "
            f"'' AS [KeyGroupby], "
            f"'' AS [groupbyValue], "
            f"CAST(COUNT(*) AS FLOAT) AS [ValueOnPrem] "
            f"FROM dbo.{Table}{where}"
        )

    def distinctQuery(
        self,
        idx: int,
        Table: str,
        Column: str,
        KeyGroupby: str,
        additionalSQLFilter: Optional[str] = None
    ) -> str:
        """
        Build a COUNT(DISTINCT column) query.

        Args:
            idx (int): Unique check index.
            Table (str): Table to query.
            Column (str): Column to count distinct values on.
            KeyGroupby (str): (unused).
            additionalSQLFilter (str, optional): Additional WHERE clause.

        Returns:
            str: T-SQL SELECT for distinct count.

        Example:
            >>> gen.distinctQuery(2, 'Sales', 'region', '', None)
            "SELECT 2 AS [index], 'sales' AS [Table], 'region' AS [Column], 'distinct' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(COUNT(DISTINCT(region)) AS FLOAT) AS [ValueOnPrem] FROM dbo.Sales"
        """
        where = f" WHERE {additionalSQLFilter}" if additionalSQLFilter else ""
        return (
            f"SELECT {idx} AS [index], "
            f"'{Table.lower()}' AS [Table], "
            f"'{Column.lower()}' AS [Column], "
            f"'distinct' AS [KeyCheck], "
            f"'' AS [KeyGroupby], "
            f"'' AS [groupbyValue], "
            f"CAST(COUNT(DISTINCT({Column})) AS FLOAT) AS [ValueOnPrem] "
            f"FROM dbo.{Table}{where}"
        )

    def sumQuery(
        self,
        idx: int,
        Table: str,
        Column: str,
        KeyGroupby: str,
        additionalSQLFilter: Optional[str] = None
    ) -> str:
        """
        Build a SUM(column) query.

        Args, Returns, Example analogous to distinctQuery but using SUM().
        """
        where = f" WHERE {additionalSQLFilter}" if additionalSQLFilter else ""
        return (
            f"SELECT {idx} AS [index], "
            f"'{Table.lower()}' AS [Table], "
            f"'{Column.lower()}' AS [Column], "
            f"'sum' AS [KeyCheck], "
            f"'' AS [KeyGroupby], "
            f"'' AS [groupbyValue], "
            f"CAST(SUM({Column}) AS FLOAT) AS [ValueOnPrem] "
            f"FROM dbo.{Table}{where}"
        )

    # … repeat similar detailed doc-strings for minQuery, maxQuery, firstdateQuery, lastdateQuery,
    # countnonnullQuery, countbyQuery, countdistinctbyQuery, sumbyQuery …

    def getCheckList(self) -> pd.DataFrame:
        """
        Return the underlying checklist DataFrame.

        Returns:
            pandas.DataFrame: The filtered checklist with columns
            ['idx','Table','Column','KeyCheck','groupbyKey','additionalSQLFilter'].
        """
        return self.checkList

    def getQuery(
        self,
        idx: int,
        Table: str,
        Column: str,
        KeyCheck: str,
        KeyGroupby: str,
        additionalSQLFilter: Optional[str] = None
    ) -> str:
        """
        Dispatch to the appropriate query builder based on KeyCheck.

        Args:
            idx (int): Check index.
            Table (str): Table name.
            Column (str): Column name (if used).
            KeyCheck (str): One of 'countrow','distinct','sum', etc.
            KeyGroupby (str): Column to group by (for *by queries).
            additionalSQLFilter (str, optional): WHERE clause fragment.

        Returns:
            str: The generated T-SQL statement.

        Example:
            >>> gen.getQuery(1,'T','C','countrow','','')
            "... COUNT(*) FROM dbo.T"
        """
        mapper = {
            'countrow': self.countrowQuery,
            'distinct': self.distinctQuery,
            'sum': self.sumQuery,
            'min': self.minQuery,
            'max': self.maxQuery,
            'firstdate': self.firstdateQuery,
            'lastdate': self.lastdateQuery,
            'countnonnull': self.countnonnullQuery,
            'countby': self.countbyQuery,
            'countdistinctby': self.countdistinctbyQuery,
            'sumby': self.sumbyQuery
        }
        return mapper[KeyCheck](idx, Table, Column, KeyGroupby, additionalSQLFilter)

    def datetimeShiftSparkToSQL(self, expression: str) -> Optional[str]:
        """
        Convert Spark CURRENT_TIMESTAMP +/- INTERVAL expressions to SQL DATEADD() format.

        Args:
            expression (str): A Spark SQL expression like 'current_timestamp() + INTERVAL 7 HOURS'.

        Returns:
            str or None: Equivalent T-SQL expression, or None if not recognized.

        Example:
            >>> gen.datetimeShiftSparkToSQL(\"to_date(current_timestamp() - INTERVAL 2 DAYS)\")
            'DATEADD(DAY, -2, GETDATE())'
        """
        # … original logic unchanged …

    def generateSQL(self) -> str:
        """
        Assemble all individual check queries into a single UNION ALL string.

        Returns:
            str: The concatenated SQL script.

        Example:
            >>> script = gen.generateSQL()
            >>> print(script[:200])  # first 200 chars
        """
        self.checkList['sql'] = self.checkList.apply(
            lambda r: self.getQuery(r['idx'], r['Table'], r['Column'],
                                    r['KeyCheck'], r['groupbyKey'],
                                    r['additionalSQLFilter']),
            axis=1
        )
        self.sql = ' UNION ALL '.join(self.checkList['sql'])
        return self.sql

    def getSQL(self) -> str:
        """
        Retrieve the generated SQL, generating it if necessary.

        Returns:
            str: The SQL script.

        Example:
            >>> sql = gen.getSQL()
        """
        if not self.sql:
            return self.generateSQL()
        return self.sql


    # TODO: implement that load one table at first then query all about that table

class UAT_Fabric(_UAT):
    """
    Execute UAT checks on Spark and optionally persist the results.
    """
    def __init__(self, WS_ID, check_LH_ID, saveResult_LH_ID, saveResult_tableName, checklist_LH_ID, checklist_csvName, saveResult=True, max_workers=4):
        """
        Initialize UAT runner with concurrency and result settings.

        Args:
            WS_ID (str): Your workspace ID.
            check_LH_ID (str): Lakehouse ID for source tables.
            saveResult_LH_ID (str): Lakehouse ID for result table.
            saveResult_tableName (str): Table name to write UAT results.
            checklist_LH_ID (str): Lakehouse ID for the checklist CSV.
            checklist_csvName (str): Checklist filename.
            saveResult (bool): Persist results? Defaults to True.
            max_workers (int): Number of parallel threads. Defaults to 4.

        Example:
            >>> fabric = UAT_Fabric(
            ...     WS_ID='ws1',
            ...     check_LH_ID='lh_src',
            ...     saveResult_LH_ID='lh_res',
            ...     saveResult_tableName='uat_results',
            ...     checklist_LH_ID='lh_chk',
            ...     checklist_csvName='checks.csv',
            ...     saveResult=True,
            ...     max_workers=4
            ... )
        """
        super().__init__(WS_ID, check_LH_ID, saveResult_LH_ID, saveResult_tableName, checklist_LH_ID, checklist_csvName)
        cTime = spark.sql("SELECT current_timestamp() + interval 7 hours").collect()[0][0]
        self.checkList['dateCheck'] = cTime
        self.checkList = self.checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey', 'dateCheck', 'additionalSQLFilter']]
        self.saveResult = saveResult
        self.max_workers = max_workers

    def addResultToTable(self, df: DataFrame) -> None:
        """
        Append a batch of UAT results to the Delta result table.

        Args:
            df (DataFrame): Must contain columns
                ['index','Table','Column','KeyCheck','KeyGroupby',
                 'groupbyValue','valueOnFabric','dateCheckFabric'].

        Example:
            >>> # assume `batch_df` is produced by runQuerySpark_TableName
            >>> fabric.addResultToTable(batch_df)
        """
        sinkPath = self.resultPath
        # sinkPath = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.saveResult_LH_ID}.Lakehouse/Tables/{self.saveResult_tableName}'
        df\
            .select(['index','Table','Column','KeyCheck','KeyGroupby','groupbyValue','valueOnFabric','dateCheckFabric'])\
            .withColumn('valueOnFabric', col('valueOnFabric').cast(DecimalType(36,5)))\
            .write.mode('append').save(sinkPath)
    
    def runQuerySpark_byRow(
        self,
        df: DataFrame,
        idx: int,
        Table: str,
        Column: str,
        KeyCheck: str,
        groupbyKey: str,
        cTime: Any,
        additionalSQLFilter: str
    ) -> DataFrame:
        """
        Execute a single UAT check against one Spark DataFrame.

        Args:
            df (DataFrame): The source table as a DataFrame.
            idx (int): Index of the check for audit.
            Table (str): Table name label.
            Column (str): Column under test (if applicable).
            KeyCheck (str): Type of check (e.g. 'countrow', 'distinct').
            groupbyKey (str): Column for grouping (for '*by' checks).
            cTime (Any): Timestamp of this check run.
            additionalSQLFilter (str): SQL WHERE clause fragment.

        Returns:
            DataFrame: One- or multi-row DataFrame with the results and audit columns.

        Example:
            >>> tbl_df = spark.read.load(path_to_table)
            >>> row_result = fabric.runQuerySpark_byRow(
            ...     tbl_df, idx=5, Table='sales', Column='amount',
            ...     KeyCheck='sum', groupbyKey='', cTime=datetime.now(), additionalSQLFilter=''
            ... )
        """
        
        # already has df in memory by df.cache()
        # Start with the base DataFrame
        if additionalSQLFilter != '':
            df = df.filter(expr(additionalSQLFilter))
        else:
            additionalSQLFilter = ''
    
        if KeyCheck == 'countrow':
            result = df.selectExpr("cast(count(*) AS DECIMAL(36,5)) AS valueOnFabric")\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(groupbyKey))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'countby':
            result = df.groupBy(groupbyKey)\
                        .agg(count('*').cast(DecimalType(36,5)).alias('valueOnFabric'))\
                        .withColumn("index", lit(idx))\
                        .withColumn("Table", lit(Table))\
                        .withColumn("Column", lit(Column))\
                        .withColumn("KeyCheck", lit(KeyCheck))\
                        .withColumn("KeyGroupby", lit(groupbyKey))\
                        .withColumn("groupbyValue", col(groupbyKey).cast("string"))\
                        .withColumn('dateCheckFabric',lit(cTime))\
                        .drop(groupbyKey)\
                        .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'countdistinctby':
            result = df.groupBy(groupbyKey)\
                        .agg(countDistinct(Column).cast(DecimalType(36,5)).alias('valueOnFabric'))\
                        .withColumn("index", lit(idx))\
                        .withColumn("Table", lit(Table))\
                        .withColumn("Column", lit(Column))\
                        .withColumn("KeyCheck", lit(KeyCheck))\
                        .withColumn("KeyGroupby", lit(groupbyKey))\
                        .withColumn("groupbyValue", col(groupbyKey).cast("string"))\
                        .withColumn('dateCheckFabric',lit(cTime))\
                        .drop(groupbyKey)\
                        .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'distinct':
            result = df.agg(countDistinct(Column).cast(DecimalType(36,5)).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(groupbyKey))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'countnonnull':
            result = df.filter(col(Column).isNotNull()).agg(count('*').cast(DecimalType(36,5)).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'firstdate':
            result = df.filter(col(Column).isNotNull()).agg(min(date_format(Column, 'yyyyMMdd')).cast(DecimalType(36,5)).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'lastdate':
            result = df.filter(col(Column).isNotNull()).agg(max(date_format(Column, 'yyyyMMdd')).cast(DecimalType(36,5)).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'max':
            result = df.filter(col(Column).isNotNull()).agg(max(Column).cast(DecimalType(36,5)).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'min':
            result = df.filter(col(Column).isNotNull()).agg(min(Column).cast(DecimalType(36,5)).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'sum':
            result = df.agg(sum(Column).cast(DecimalType(36,5)).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'sumby':
            result = df.groupBy(groupbyKey)\
                        .agg(sum(Column).cast(DecimalType(36,5)).alias('valueOnFabric'))\
                        .withColumn("index", lit(idx))\
                        .withColumn("Table", lit(Table))\
                        .withColumn("Column", lit(Column))\
                        .withColumn("KeyCheck", lit(KeyCheck))\
                        .withColumn("KeyGroupby", lit(groupbyKey))\
                        .withColumn("groupbyValue", col(groupbyKey).cast("string"))\
                        .withColumn('dateCheckFabric',lit(cTime))\
                        .drop(groupbyKey)\
                        .withColumn('filter', lit(additionalSQLFilter))
    
        #special exception
        if groupbyKey.lower() == 'saledate':
            result = result.withColumn('groupbyValue', date_format(col("groupbyValue").cast(TimestampType()), 'yyyyMMdd'))
    
        return result

    def runQuerySpark_TableName(self, TableName: str) -> DataFrame:
        """
        Run all UAT checks for a single table and optionally save the batch.

        Args:
            TableName (str): Name of the table to validate.

        Returns:
            DataFrame: Concatenated results of each row-level check.

        Example:
            >>> df_sales = fabric.runQuerySpark_TableName('sales')
            >>> df_sales.show()
        """
        referenceTable = self.checkList
        referenceTable_filter = referenceTable[referenceTable['Table'].apply(lambda x: x.lower())==TableName.lower()]
        checkedTable = spark.read.load(self.getCheckedTablePath(TableName.lower()))
    
        results = []
    
        for rowIdx in tqdm(referenceTable_filter.index, desc=TableName,leave=True):
            resultRow = self.runQuerySpark_byRow(checkedTable,*referenceTable_filter.loc[rowIdx])
            # def runQuerySpark_byRow(self, df,idx, Table, Column, KeyCheck, groupbyKey, cTime, additionalSQLFilter):
            results.append(resultRow)
        
        result = results[0]
        result = result.withColumn('valueOnFabric', col('valueOnFabric').cast(DecimalType(36,5)))
        for r in results[1:]:
            r = copySchemaByName(r,result)
            r = r.withColumn('valueOnFabric', col('valueOnFabric').cast(DecimalType(36,5)))
            result = result.unionByName(r)
        
        if self.saveResult:
            self.addResultToTable(result)
    
        return result

    def runQuerySpark(self) -> DataFrame:
        """
        Execute all UAT checks across all tables concurrently.

        Returns:
            DataFrame: The full union of all table-level results.
        
        Example:
            >>> full_results = fabric.runQuerySpark()
            >>> full_results.count()
        """
    
        allTable = self.checkList['Table'].unique()
        numtable = len(allTable)
    
        print('query processing ...')
        with ThreadPoolExecutor(max_workers = self.max_workers) as p:
            results = list(p.map(self.runQuerySpark_TableName,allTable))
        
        print('query successed')

        result = results[0]
        for r in results[1:]:
            r = copySchemaByName(r,result)
            result = result.unionByName(r)
        return result.withColumn('valueOnFabric',col('valueOnFabric').cast(DoubleType()))