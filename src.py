from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from typing import Union, List, Tuple, Any
import notebookutils
import pandas as pd
import numpy as np

spark = SparkSession.builder\
        .appName("fusion")\
        .getOrCreate()

class _AuditLog_Fusion:

    class logger:
        def __init__(self, **kwargs):
            self._data = kwargs
    
        def __getitem__(self, key):
            return self._data[key]
    
        def __setitem__(self, key, value):
            if key not in self._data:
                raise KeyError(f"Cannot add new key: {key}")
            self._data[key] = value
    
        def __delitem__(self, key):
            raise KeyError(f"Cannot delete key: {key}")
    
        def __iter__(self):
            return iter(self._data)
    
        def __len__(self):
            return len(self._data)
    
        def __repr__(self):
            return repr(self._data)
    
    def __init__(self, columns: Union[List[str], Tuple[str, ...]], WS_ID: str, TABLE_NAME_to_check:str, AUDIT_TABLE_NAME:str, LH_ID_to_check: str, LH_ID_audit: str = None, schema: str = None):
        '''
        - if `LH_ID_audit` is not given, it is  LH_ID_to_check automatically, i.e. audit table is in the same lakehouse as that of
        - if using lakehouse with Schema, please provide `schema` parameter
        '''
        self.WS_ID = WS_ID
        self.TABLE_NAME_to_check = TABLE_NAME_to_check
        self.AUDIT_TABLE_NAME = AUDIT_TABLE_NAME
        self.LH_ID_to_check = LH_ID_to_check
        self.LH_ID_audit = LH_ID_audit if LH_ID_audit else LH_ID_to_check
        self.schema = schema
        self.fixColumns = {'STARTTIME','ENDTIME','AUDITKEY','STATUS_ACTIVITY'}
        self.columns = tuple(set(columns).union(self.fixColumns))
        
        if self.schema:    
            self.PATH_TO_AUDIT_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_audit}/Tables/{self.schema}/{self.AUDIT_TABLE_NAME}'
            self.PATH_TO_CHECKED_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_to_check}/Tables/{self.schema}/{self.TABLE_NAME_to_check}'
        else:
            self.PATH_TO_AUDIT_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_audit}/Tables/{self.AUDIT_TABLE_NAME}'
            self.PATH_TO_CHECKED_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_to_check}/Tables/{self.TABLE_NAME_to_check}'
        
        # if not notebookutils.fs.exists(self.PATH_TO_AUDIT_TABLE):
        #     raise FileExistsError(f'Create you audit table first at path {self.PATH_TO_AUDIT_TABLE}')
        # if not notebookutils.fs.exists(self.PATH_TO_CHECKED_TABLE):
        #     raise FileExistsError(f'your given table does not exists at path {self.PATH_TO_CHECKED_TABLE}')
    
        self.log = self.logger(**{column: None for column in self.columns})
        self.log['STATUS_ACTIVITY'] = 'Not start'

    def setKeys(self, initConfig: dict[str, Any]):
        assert set(initConfig.keys()).issubset(set(self.columns).difference()), f'initConfig must have the columns in {self.columns}'
        for column in initConfig:
            self.log[column] = initConfig[column]

    def setKey(self, key: str, value: Any):
        assert key in self.columns, f'key must be in {self.columns}'
        self.log[key] = value
        
    def initialDetail(self, initConfig: dict[str, Any]):
        self.setKeys(initConfig)

    def getKey(self):
        return self.columns
    
    def getLog(self):
        return self.log
        
    def __str__(self):
        out = ''
        for key in self.columns:
            out += f'{key}: {self.log[key]}\n'
        return out
    
    def __repr__(self):
        return str(self.log)
    
    def endSuccess(self):
        self.log['STATUS_ACTIVITY'] = 'Success'
        self._endAuditLog()
        print(self)

        
    def endFail(self, errorCode: str, errorMessage: str):
        self.log['STATUS_ACTIVITY'] = 'Fail'
        self.log['ERRORCODE'] = errorCode
        self.log['ERRORMESSAGE'] = errorMessage
        self._endAuditLog()
        print(self)

    def _endAuditLog(self):
        # write to audit table
        self.log['ENDTIME'] = str(datetime.now() + timedelta(hours=7))
        row = {}
        for key in self.log:
            row[key] = [str(self.log[key])]
        data_tuples = list(zip(*row.values()))
        df = spark.createDataFrame(data_tuples, schema=list(row.keys()))
        df.write.mode('append').save(self.PATH_TO_AUDIT_TABLE)
        return df

    def getAuditLogTable(self):
        return spark.read.load(self.PATH_TO_AUDIT_TABLE)
    
    def countBefore(self):
        if self.log['COUNTROWSBEFORE']:
            raise ValueError('COUNTROWSBEFORE already exist')
        self.log['COUNTROWSBEFORE'] = spark.read.load(self.PATH_TO_CHECKED_TABLE).count()

    def countAfter(self):
        self.log['COUNTROWSAFTER'] = spark.read.load(self.PATH_TO_CHECKED_TABLE).count()

    def getAllPath(self):
        return {'PATH_TO_AUDIT_TABLE':self.PATH_TO_AUDIT_TABLE, 'PATH_TO_CHECKED_TABLE':self.PATH_TO_CHECKED_TABLE}

    def startAudit(self):
        self.log['STARTTIME'] = str(datetime.now() + timedelta(hours=7))
        self.log['STATUS_ACTIVITY'] = 'logging ...'

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

    def initialDetail(self,  pipelineName: str, pipelineId: str, TriggerType: str, functionName: str):
        super().initialDetail({
            'PIPELINENAME': pipelineName, 
            'PIPELINERUNID': pipelineId, 
            'TRIGGERTYPE': TriggerType, 
            'TABLE_NAME': self.TABLE_NAME_to_check, 
            'FUNCTION_NAME': functionName
        })
        self.startAudit()
        self.log['AUDITKEY'] = self.log['PIPELINENAME'] + '-' + self.log['TABLE_NAME'] + '-' + str(self.log['STARTTIME']).replace(' ','_').replace(':','_')

    def execute(self, ETL_func, raiseError = True):
        try:
            self.countBefore()
            ETL_func()
            self.countAfter()
            self.endSuccess()
            return True
        except Exception as e:
            self.endFail(errorCode = '-', errorMessage = e)
            if not raiseError:
                raise e

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
    
    def create_table(self, table_name: str, lh_name: str, column_datatype_map: dict[str, DataType], precision_scale_map: dict[str, tuple], table_partition_columns: List[str] =None):
        '''
        column_datatype_map should be a dict with column name as key and datatype function from `pyspark.sql.types` as value.
        precision_scale_map should be a dict with column name as key and a tuple of (precision, scale) as value of decimal type; in case of other type it can be `(None, None)` or omitted.
        '''
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

    def readMetaFile(self):
        if  self.format == 'csv':
            meta_df = spark.read.option("header", "true").option("delimiter", ",").csv(self.META_PATH).toPandas().set_index(['TableName','LakehouseName'], drop=False)
            return meta_df
        elif self.format == 'delta':
            meta_df = spark.read.format("delta").load(self.META_PATH).toPandas().set_index(['TableName','LakehouseName'], drop=False)
            return meta_df

    def create_all(self, meta_table: pd.DataFrame):
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
                    precision_scale_map[column_name] = (row['Precision'], row['Scale'])
                if row['forPartition'] == 1:
                    table_partition_columns.append(column_name)
            self.create_table(table, lakehouse_name, column_datatype_map, precision_scale_map, table_partition_columns)

    def run(self):
        self.meta_table = self.readMetaFile()
        self.create_all(self.meta_table)

class utils:

    @staticmethod
    def trim_string_columns(df: DataFrame) -> DataFrame:
        """
        Trims all string columns in the given DataFrame.
        Parameters:
        df (DataFrame): Input DataFrame with string columns to be trimmed.
        Returns:
        DataFrame: A new DataFrame with trimmed string columns.
        """
        # Get the list of string columns
        string_columns = [field.name for field in df.schema.fields if field.dataType.typeName() == 'string']
        # Trim each string column
        for column in string_columns:
            df = df.withColumn(column, trim(col(column)))
        return df
 
    @staticmethod
    def fillNaAll(df: DataFrame) -> DataFrame:
        """
        Fills all null (NA) values in the given DataFrame with default values based on column data types.

        Args:
            df (DataFrame): The input PySpark DataFrame whose null values are to be filled.

        Returns:
            DataFrame: A new DataFrame with null values replaced by default values for each column type.

        Raises:
            TypeError: If a column's data type does not have a defined default fill value.

        Default fill values:
            - StringType: ''
            - ShortType: 0
            - IntegerType: 0
            - DoubleType: 0.0
            - TimestampType: '1970-01-01 00:00:00'
            - LongType: 0
            - DecimalType: 0.0
        """
        fillnaDefault = {
            StringType: '',
            ShortType: 0,
            IntegerType: 0,
            DoubleType: 0.0,
            TimestampType: '1970-01-01 00:00:00',
            LongType:0,
            DecimalType:0.0,
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
        Fills all columns of string type in the given DataFrame with 'NA' where values are null.

        Args:
            df (DataFrame): The input Spark DataFrame to process.
            value (str): The value to replace nulls in string columns. Default is 'NA'.

        Returns:
            DataFrame: A new DataFrame where all columns of string type have null values replaced with 'NA'.

        Example:
            >>> df = spark.createDataFrame([("Alice", None), (None, "Bob")], ["name", "friend"])
            >>> fillNaAllStringType(df).show()
            +-----+------+
            | name|friend|
            +-----+------+
            |Alice|    NA|
            |   NA|   Bob|
            +-----+------+
        """
        fillnaDefault = {
            StringType: value,
        }
        fill_values = {}
        for field in df.schema.fields:
            field_type = type(field.dataType)
            if field_type in fillnaDefault:
                fill_values[field.name] = fillnaDefault[field_type]
        return df.fillna(fill_values)

    @staticmethod
    def copySchemaByName(df: DataFrame, fromDf: DataFrame):
        '''
        schemaField can be get by df.schema.fields
        '''
        dfCol = df.columns
        for column in fromDf.schema.fields:
            if column.name in dfCol: # [] TODO: don't forget to add to other Notebook
                df = df.withColumn(column.name,col(column.name).cast(column.dataType))
        return df

    @staticmethod
    def getSetColumn(df,columnName):
        return set(df.select(columnName).toPandas()[columnName])

    @staticmethod
    def getCountColumn(df,columnName):
        return df.select(columnName).toPandas()[columnName].value_counts()

    @staticmethod
    def trackSizeTable(df,detail=None,schema = False,table=False):
        if detail:
            print(detail,end=': size = ')
        
        numrow = df.count()
        print(f'({numrow}, {len(df.columns)})')
        
        if schema:
            df.printSchema()
        
        if table:
            df.show()

    @staticmethod
    def trackSizeOnLake(tablePath):
        numRow = spark.sql(
        f"""
        SELECT COUNT(*) FROM {tablePath}
        """).collect()[0][0]

        numCol = spark.sql(
        f"""
        DESCRIBE {tablePath}
        """).count()

        print(tablePath,end=': size = ')
        print(f'({numRow}, {numCol})')

    @staticmethod
    def getSizeOnLake(tablePath):
        numRow = spark.sql(
        f"""
        SELECT COUNT(*) FROM {tablePath}
        """).collect()[0][0]

        numCol = spark.sql(
        f"""
        DESCRIBE {tablePath}
        """).count()

        return numRow, numCol
    
    @staticmethod
    def scdType2(sourceTable, targetTable, primarykey, comparedColumns=None, sortTimeColumn = 'TimeStamp', startDate= 'startDate', endDate='endDate', activeFlag='activeFlag'):
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
        ...     (1, "A", "2024-05-01", "9999-12-31", True),
        ...     (2, "C", "2024-05-01", "9999-12-31", True),
        ... ], ["id", "value", "startDate", "endDate", "activeFlag"])
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
    def get_ws_id():
        """
        Get the workspace ID from the notebook context.
        """
        return spark.conf.get('trident.workspace.id')

    @staticmethod
    def get_lh_id(WS_ID, lh_name, caseSensitive=True):
        """
        Get the lakehouse ID from the workspace ID and lakehouse name.
        """
        if caseSensitive:
            return notebookutils.lakehouse.get(lh_name,WS_ID)['id']
        else:
            listAllLH = notebookutils.lakehouse.list(WS_ID)
            for lh in listAllLH:
                if lh['displayName'].lower() == lh_name.lower():
                    return lh['id']
            
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
    def __init__(self, WS_ID, checklist_LH_ID, checklist_csvName, saveQuery_fileName):
        super().__init__(WS_ID=WS_ID, check_LH_ID='', saveResult_LH_ID='', saveResult_tableName='', checklist_LH_ID=checklist_LH_ID, checklist_csvName=checklist_csvName)
        self.checkList = self.checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey', 'additionalSQLFilter']]
        self.sql = None

    def countrowQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'' AS [Column], 'countrow' AS [KeyCheck],'' AS [KeyGroupby], '' AS [groupbyValue], CAST(COUNT(*) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))}"
    
    def distinctQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'distinct' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(COUNT(DISTINCT({Column})) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))}"
    
    def sumQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'sum' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(SUM({Column}) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))}"
    
    def minQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'min' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(MIN({Column}) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))}"
    
    def maxQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'max' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(MAX({Column}) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))}"
    
    def firstdateQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'firstdate' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(FORMAT(CAST(MIN({Column}) AS DATETIME), 'yyyyMMdd') AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table} WHERE {Column} IS NOT NULL{bool(additionalSQLFilter)*(' AND '+str(additionalSQLFilter))}"
    
    def lastdateQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'lastdate' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(FORMAT(CAST(MAX({Column}) AS DATETIME), 'yyyyMMdd') AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table} WHERE {Column} IS NOT NULL{bool(additionalSQLFilter)*(' AND '+str(additionalSQLFilter))}"
    
    def countnonnullQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table], '{Column.lower()}' AS [Column], 'countnonnull' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(COUNT({Column}) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table} WHERE {Column} IS NOT NULL{bool(additionalSQLFilter)*(' AND '+str(additionalSQLFilter))}"
    
    def countbyQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table], '' AS [Column], 'countby' AS [KeyCheck], '{KeyGroupby}' AS [KeyGroupby], {KeyGroupby} AS [groupbyValue], CAST(COUNT(*) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))} GROUP BY {KeyGroupby}"

    def countdistinctbyQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table], '{Column.lower()}' AS [Column], 'countdistinctby' AS [KeyCheck], '{KeyGroupby}' AS [KeyGroupby], {KeyGroupby} AS [groupbyValue], CAST(COUNT(DISTINCT({Column})) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))} GROUP BY {KeyGroupby}"
    
    def sumbyQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table], '{Column.lower()}' AS [Column], 'sumby' AS [KeyCheck], '{KeyGroupby}' AS [KeyGroupby], {KeyGroupby} AS [groupbyValue], CAST(SUM({Column}) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))} GROUP BY {KeyGroupby}"

    def getCheckList(self):
        return self.checkList
    
    def getQuery(self, idx, Table, Column, KeyCheck, KeyGroupby, additionalSQLFilter=None):
        mapper = {
        'countrow':self.countrowQuery,
        'distinct':self.distinctQuery,
        'sum': self.sumQuery,
        'min':self.minQuery,
        'max':self.maxQuery,
        'firstdate':self.firstdateQuery,
        'lastdate':self.lastdateQuery,
        'countnonnull':self.countnonnullQuery,
        'countby': self.countbyQuery,
        'countdistinctby': self.countdistinctbyQuery,
        'sumby': self.sumbyQuery
        }
        return mapper[KeyCheck](idx, Table, Column, KeyGroupby, additionalSQLFilter)

    def datetimeShiftSparkToSQL(self, expression):
        if re.match(r".*to_date\(current_timestamp.*",expression):
            result = 'GETDATE()'
            # matchHour = re.match(r".*([+-])[ ]*INTERVAL[ ]+([0-9]+)[ ]+HOURS.*",expression)
            # if matchHour:
            #     intervalHour = (matchHour.group(1) + matchHour.group(2)).replace("+",'')
            #     result = f'DATEADD(HOUR, {intervalHour}, {result})'
            matchDay = re.match(r".*([+-])[ ]*INTERVAL[ ]+([0-9]+)[ ]+DAYS.*",expression)
            if matchDay:
                intervalDay = (matchDay.group(1) + matchDay.group(2)).replace("+",'')
                result = f'DATEADD(DAY, {intervalDay}, {result})'
            return result
        else:
            return None

    def generateSQL(self):
                                                                    # ['idx',          'Table',      'Column',      'KeyCheck',      'groupbyKey',      'additionalSQLFilter']
        self.checkList['sql'] = self.checkList.apply(lambda row: self.getQuery(row['idx'], row['Table'], row['Column'], row['KeyCheck'], row['groupbyKey'], row['additionalSQLFilter']), axis=1)
        self.sql =  ' UNION ALL '.join(self.checkList['sql'])
        return self.sql

    def getSQL(self):
        if not self.sql:
            self.generateSQL()
        return self.sql

    # TODO: implement that load one table at first then query all about that table

class UAT_Fabric(_UAT):
    def __init__(self, WS_ID, check_LH_ID, saveResult_LH_ID, saveResult_tableName, checklist_LH_ID, checklist_csvName, saveResult=True, max_workers=4):
        super().__init__(WS_ID, check_LH_ID, saveResult_LH_ID, saveResult_tableName, checklist_LH_ID, checklist_csvName)
        cTime = spark.sql("SELECT current_timestamp() + interval 7 hours").collect()[0][0]
        self.checkList['dateCheck'] = cTime
        self.checkList = self.checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey', 'dateCheck', 'additionalSQLFilter']]
        self.saveResult = saveResult
        self.max_workers = max_workers

    def addResultToTable(self, df):
        sinkPath = self.resultPath
        # sinkPath = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.saveResult_LH_ID}.Lakehouse/Tables/{self.saveResult_tableName}'
        df\
            .select(['index','Table','Column','KeyCheck','KeyGroupby','groupbyValue','valueOnFabric','dateCheckFabric'])\
            .withColumn('valueOnFabric', col('valueOnFabric').cast(DecimalType(36,5)))\
            .write.mode('append').save(sinkPath)
    
    def runQuerySpark_byRow(self, df,idx, Table, Column, KeyCheck, groupbyKey, cTime, additionalSQLFilter):
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

    def runQuerySpark_TableName(self, TableName):
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

    def runQuerySpark(self):
    
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