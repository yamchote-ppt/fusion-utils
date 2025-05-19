# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from typing import Union, List, Tuple, Any
import notebookutils
import pandas as pd
import os
import env.Utils as Utils
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor
import ast
import re
import pytz


ict_timezone = pytz.timezone('Asia/Bangkok')

# from tabulate import tabulate

spark = SparkSession.builder\
        .appName("ETL")\
        .getOrCreate()

class ETL_base:

    def __init__(self):
        self.log = {}
        self.TYPE_MAPPER = {
            'varchar':StringType,
            'varbinary':BinaryType,
            'bigint': LongType,
            'bit': BooleanType,
            'date': DateType,
            'Date': DateType,
            'datetime': TimestampType,
            'datetime2': TimestampType,
            'decimal': DecimalType,
            'float': DecimalType,
            'int': IntegerType,
            'smallint': ShortType,
            'time': StringType,
            'Time': StringType,
        }

    def changeName(self, df, changeNameMapper):
        for column in changeNameMapper:
            df = df.withColumnRenamed(column,changeNameMapper[column])
        return df

    def masking(self, df, maskingMapper, typeMapper=None):
        df = df.withColumns(
            {column:lit('xxxxxx') for column in maskingMapper if maskingMapper[column] == 'X'}
        )
        return df
    
    def changeType_ver1(self,df, typeMapper, precisionMapper, scaleMapper):
        for column in typeMapper:
            if typeMapper[column] == 'decimal':
                precision = int(precisionMapper[column])
                scale = int(scaleMapper[column])
                df = df.withColumn(column,col(column).cast(DecimalType(precision,scale)))
            else:
                df = df.withColumn(column,col(column).cast(self.TYPE_MAPPER[typeMapper[column]]()))
        return df

    def changeType(self,df, typeSource, typeMapper, precisionMapper, scaleMapper):
        for column in typeMapper:
            if typeMapper[column].typeName() == 'decimal':
                precision = int(precisionMapper[column]) if precisionMapper[column] else 38
                scale = int(scaleMapper[column]) if scaleMapper[column] else 10
                df = df.withColumn(column,col(column).cast(DecimalType(precision,scale)))
            elif typeMapper[column].typeName() == 'timestamp':
                if typeSource[column].typeName() == 'decimal':
                    df = df.withColumn(column,to_timestamp(col(column).cast(LongType()).cast(StringType()),"yyyyMMddHHmmss"))
                else:
                    df = df.withColumn(column,col(column).cast(typeMapper[column]()))
            elif typeMapper[column].typeName() == 'date':
                if typeSource[column].typeName() == 'string':
                    df = df.withColumn(column, to_date(when(col(column)=="00000000","19000101").otherwise(col(column)),"yyyyMMdd"))
                else:
                    df = df.withColumn(column,col(column).cast(typeMapper[column]()))
            elif typeMapper[column].typeName() == 'boolean':
                if typeSource[column].typeName() == 'string':
                    df = df.withColumn(column, (col(column)=='X').cast(BooleanType()))
                else:
                    df = df.withColumn(column, col(column).cast(typeMapper[column]()))
            else:
                df = df.withColumn(column,col(column).cast(typeMapper[column]()))
        return df
    
    def fillNa(self, df, columns):
        fillnaDefault = {
            'string': '',
            'integer': 0,
            'decimal': 0.0,
            'timestamp': '1900-01-01 00:00:00',
            'boolean': True,
            'smallint': 0,
            'int': 0,
            'float': 0.0,
            'bigint': 0,
            'varchar':'',
            'varbinary':'0',
            'date': '1900-01-01',
            'datetime': '1970-01-01 00:00:00',
            'binary': 0
        }
        typeMap = {field.name: re.match(r'([a-zA-Z]+)' ,field.dataType.simpleString()).group(1) for field in df.schema.fields}
        fill_values = {}

        for column in columns:
            fill_values[column] = fillnaDefault[typeMap[column]]

        return df.fillna(fill_values)
    
    def _scdType2_old(self, source, target, on, startDate= 'startDate', endDate='endDate', activeFlag='activeFlag', printStatus=False):
        '''
        Assume that column names of source and target are the same
        In this case, we assume that the source is the latest data: no need to check the change in source
        '''
        if isinstance(on, str):
            on = [on]
        
        target_disable = target.filter(~col(activeFlag)).withColumn('STATUS', lit('SAME'))
        target_enable = target.filter(col(activeFlag))

        source = source.withColumnsRenamed({column:column+'_source' for column in source.columns if column not in on+[startDate,endDate,activeFlag]}) # rename all columns except join column and marking to ..._source
        inner = target_enable.join(source, on=on, how='inner') # records that are in both source and target (columns with "_source" are new records from source)

        old_inner = inner.select(target.columns).withColumn(endDate, current_date()).withColumn(activeFlag, lit(False)).withColumn('STATUS', lit('DISABLE')) # records that are in both source and target: update old records
        new_inner = inner.select(source.columns).withColumn(startDate, current_date()).withColumn(endDate, to_date(lit('9999-12-31'))).withColumn(activeFlag, lit(True))\
            .withColumnsRenamed({column:column.replace('_source',"") for column in source.columns if '_source' in column}).withColumn('STATUS', lit('UPDATE'))

        left_anti = target_enable.join(source, on=on, how='left_anti').withColumn('STATUS', lit('SAME')) # records that are in target but not in source: keep the same
        right_anti = source.join(target, on=on, how='left_anti').withColumn(startDate, current_date()).withColumn(endDate, to_date(lit('9999-12-31'))).withColumn(activeFlag, lit(True))\
            .withColumnsRenamed({column:column.replace('_source',"") for column in source.columns if '_source' in column}).withColumn('STATUS', lit('INSERT')) # records that are in source but not in target: add new records

        result = target_disable.unionByName(old_inner).unionByName(new_inner).unionByName(left_anti).unionByName(right_anti).cache()
        
        if printStatus:
            result.groupBy('STATUS').count().show()

        return result.drop('STATUS')
    
    def scdType2(self,sourceTable, targetTable, primarykey, comparedColumns=None, sortTimeColumn = 'TimeStamp', startDate= 'startDate', endDate='endDate', activeFlag='activeFlag'):
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

    def reload(self, df, path):
        df.write.mode('overwrite').option('mergeSchema','true').save(path)
        return spark.read.load(path)
    
    # def scdType2(source, target, on, startDate= 'startDate', endDate='endDate', activeFlag='activeFlag', printStatus=False):
    #     '''
    #     Assume that column names of source and target are the same
    #     In this case, we assume that the source is the latest data: no need to check the change in source
    #     '''
    #     if isinstance(on, str):
    #         on = [on]

    #     source = source.drop_duplicates()
    #     target_disable = target.filter(~col(activeFlag)).withColumn('STATUS', lit('SAME'))
    #     target_enable = target.filter(col(activeFlag))

    #     source = source.withColumnsRenamed({column:column+'_source' for column in source.columns if column not in on+[startDate,endDate,activeFlag]}) # rename all columns except join column and marking to ..._source
    #     inner = target_enable.join(source, on=on, how='inner') # records that are in both source and target (columns with "_source" are new records from source)

    #     old_inner = inner.select(target.columns).withColumn(endDate, current_date()).withColumn(activeFlag, lit(False)).withColumn('STATUS', lit('DISABLE')) # records that are in both source and target: update old records
    #     new_inner = inner.select(source.columns).withColumn(startDate, current_date()).withColumn(endDate, to_date(lit('9999-12-31'))).withColumn(activeFlag, lit(True))\
    #         .withColumnsRenamed({column:column.replace('_source',"") for column in source.columns if '_source' in column}).withColumn('STATUS', lit('UPDATE'))

    #     left_anti = target_enable.join(source, on=on, how='left_anti').withColumn('STATUS', lit('SAME')) # records that are in target but not in source: keep the same
    #     right_anti = source.join(target, on=on, how='left_anti').withColumn(startDate, current_date()).withColumn(endDate, to_date(lit('9999-12-31'))).withColumn(activeFlag, lit(True))\
    #         .withColumnsRenamed({column:column.replace('_source',"") for column in source.columns if '_source' in column}).withColumn('STATUS', lit('INSERT')) # records that are in source but not in target: add new records

    #     result = target_disable.unionByName(old_inner).unionByName(new_inner).unionByName(left_anti).unionByName(right_anti).cache()
        
    #     if printStatus:
    #         result.groupBy('STATUS').count().show()

    #     return result.drop('STATUS')

    # def reload(self, df, path):
    #     df.write.mode('overwrite').save(path)
    #     return spark.read.load(path)

# class BrToSil(ETL_base):

#     def __init__(self,config):
#         super().__init__()
#         self.WS_ID = config['WS_ID']
#         self.BRONZE_LH_ID = config['BRONZE_LH_ID']
#         self.SILVER_LH_ID = config['SILVER_LH_ID']
#         # self.MAPPER_TABLE_NAME = config['MAPPER_TABLE_NAME']
        
#         # self.CATEGORY = config['CATEGORY']
        
#         self.ABSOLUTE_MAPPER_TABLE_NAME = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BRONZE_LH_ID}/Tables/mapper/{self.MAPPER_TABLE_NAME}'

#         self.MAPPER = spark.read.load(self.ABSOLUTE_MAPPER_TABLE_NAME).filter(col('Category')==self.CATEGORY).toPandas()
#         self.MAPPER['SilverTableNameChange'] = self.MAPPER['SilverTableNameChange'].fillna(self.MAPPER['SilverTableName'])
#         self.MAPPER['FieldNameChange'] = self.MAPPER['FieldNameChange'].fillna(self.MAPPER['FieldName'])
#         self.MAPPER['TypeChange'] = self.MAPPER['TypeChange'].str.strip().apply(lambda x: None if x == '' else x).fillna(self.MAPPER['Type'])

#         self.TableInfo = self.MAPPER.copy()[['BronzeSchema', 'BronzeTable', 'SilverSchema','SilverTableNameChange','TableType', 'Category']].drop_duplicates() # TODO: add PK
#         self.TableInfo = self.TableInfo.set_index(['BronzeSchema', 'BronzeTable', 'SilverSchema','SilverTableNameChange'])

#         self.MAPPER = self.MAPPER[['BronzeSchema', 'BronzeTable', 'SilverSchema','SilverTableNameChange', 'BronzeColumn', 'FieldNameChange','TypeChange', 'DynamicMasking', 'TableType', 'DQRuleCase1_Null','DQRuleCase2_Decimal','DQRuleCase3_Int','DQRuleCase4_Bit','PK']]

#         self.PK_df = self.MAPPER[self.MAPPER['PK']=='X'][['BronzeSchema', 'BronzeTable', 'SilverSchema','SilverTableNameChange','FieldNameChange']].set_index(['BronzeSchema', 'BronzeTable', 'SilverSchema','SilverTableNameChange']).sort_index()

#         self.MAPPER = self.MAPPER.set_index(['BronzeSchema', 'BronzeTable', 'SilverSchema','SilverTableNameChange'])
#         self.MAPPER['TypeChange'] = self.MAPPER['TypeChange'].str.lower()
#         self.MAPPER['Type'] = self.MAPPER['TypeChange'].str.extract(r'([a-zA-Z]+)')
#         self.MAPPER['precision'] = self.MAPPER['TypeChange'].str.extract(r'.*\(([0-9]+)\,*[0-9]*\)')
#         self.MAPPER['scale'] = self.MAPPER['TypeChange'].str.extract(r'.*\([0-9]+\,([0-9]+)\)')
#         self.MAPPER = self.MAPPER.drop(columns=['TypeChange'])

#         self.BronzePath = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BRONZE_LH_ID}/Tables/'
#         self.SilverPath = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SILVER_LH_ID}/Tables/'

#     def transformTable(self,BronzeSchema, BronzeTableName):

#         # BronzeTablePath = os.path.join(self.BronzePath,BronzeSchema,BronzeTableName)
#         # bronzeTable = spark.read.load(BronzeTablePath)
#         # self.log['bronzeTable'] = bronzeTable
#         # df = self.MAPPER.sort_index()
#         # df = df.loc[(BronzeSchema, BronzeTableName)]

#         # # change column name according to mapper
#         # df = df.reset_index(drop = True)
#         # df = df.set_index('BronzeColumn')
#         # changeNameMapper = dict(df['FieldNameChange'])
#         # bronzeTable = self.changeName(bronzeTable, changeNameMapper)

#         df = df.reset_index(drop = True)
#         df = df.set_index('FieldNameChange')

#         # masking
#         # maskingMapper = dict(df['DynamicMasking'])
#         # bronzeTable = self.masking(bronzeTable,maskingMapper=maskingMapper)

#         # type (DQ Rule #2 - #4)
#         # typeMapper = dict(df['Type'])
#         # precisionMapper = dict(df['precision'])
#         # scaleMapper =dict(df['scale'])
#         # bronzeTable = self.changeType_ver1(bronzeTable, typeMapper, precisionMapper, scaleMapper)

#         # control digit
#         # maskMapper = None
#         # numDigitMapper = None
#         # df = self.controlDigit(df, maskMapper, numDigitMapper)

#         # null value (DQ Rule #1)
#         bronzeTable = Utils.fillNaAll(bronzeTable)

#         return bronzeTable

#     def saveTransaction(self,transformedBronzeTable,SilverSchema,SilverTableNameChange,PK):
#         transformedBronzeTable.write.mode('overwrite').save(os.path.join(self.SilverPath,SilverSchema,SilverTableNameChange))

#     def saveMaster(self,transformedBronzeTable,SilverSchema,SilverTableNameChange,PK):
#         transformedBronzeTable.withColumn('startDate',current_date()).withColumn('endDate',to_date(lit('9999-12-31'))).withColumn('activeFlag',lit(True)).write.mode('overwrite').option('overwriteSchema','true').save(os.path.join(self.SilverPath,SilverSchema,SilverTableNameChange))

#     def save(self, transformedBronzeTable, SilverSchema, SilverTableNameChange, TableType, PK): #for demo (need of PK and capture change column for SCD)
#         if TableType.strip().lower() == 'master':
#             self.saveMaster(transformedBronzeTable,SilverSchema,SilverTableNameChange,PK)
#         elif TableType.strip().lower() == 'transaction':
#             self.saveTransaction(transformedBronzeTable,SilverSchema,SilverTableNameChange,PK)
#         else:
#             raise NameError('not correct `TableType`')
    
#     def executeByTable(self,bronzeSchema, bronzeTableName, silverSchema, silverTableName):
#         TableType = self.TableInfo.loc[(bronzeSchema, bronzeTableName, silverSchema, silverTableName),'TableType']
#         PK = self.PK_df.loc[(bronzeSchema, bronzeTableName, silverSchema, silverTableName),'FieldNameChange'].to_list() # list of Primary Key(s)
#         # print(f'ETL {bronzeSchema}.{bronzeTableName} ----> {silverSchema}.{silverTableName} as {TableType} processing', end=': ')
#         transformedBronzeTable = self.transformTable(bronzeSchema, bronzeTableName)
#         self.save(transformedBronzeTable, silverSchema, silverTableName, TableType, PK)
#         # print(f'successed')

#     def execute(self, max_workers = 1):
#         # TODO []: make this process compute parallel to speed it up
#         allSchema = self.MAPPER.index.unique()
#         for tup in allSchema:
#             try:
#                 if tup[1] not in []:
#                     bronzeSchema, bronzeTableName, silverSchema, silverTableName = tup
#                     self.executeByTable(bronzeSchema, bronzeTableName, silverSchema, silverTableName)
                    
#             except Exception as e:
#                 bronzeSchema, bronzeTableName, silverSchema, silverTableName = tup
#                 TableType = self.TableInfo.loc[(bronzeSchema, bronzeTableName, silverSchema, silverTableName),'TableType']
#                 print(f'ETL {bronzeSchema}.{bronzeTableName} ----> {silverSchema}.{silverTableName} as {TableType} processing', end=': ')
#                 print(f'\n\tFAIL as {e}')

class BrToSil_ver2(ETL_base):

    def __init__(self,config):
        """
        Initializes the ETL class with the given configuration.
        Args:
            config (dict): A dictionary containing the configuration parameters. 
            Required keys:
                - 'WS_ID' (str): Workspace ID for the Azure Fabric storage.
                - 'METADATA_LH_ID' (str): Metadata Lakehouse ID.
                - 'METADATA_TABLE_NAME' (str): Name of the metadata table.
                - 'BRONZE_LH_ID' (str): Bronze Lakehouse ID.
                - 'SILVER_LH_ID' (str): Silver Lakehouse ID.
            Optional keys:
                - 'CATEGORY' (str, optional): Category of the ETL process. Defaults to None.
                - 'INGESTION_TYPE' (str, optional): Type of ingestion process. Defaults to 'BronzeToSilver'.
                - 'optionalFilter' (dict, optional): A dictionary specifying additional filters to apply on the metadata. 
                  The keys must match the columns in the metadata table.
                - 'isInitial' (bool, optional): Flag indicating whether this is an initial run. Defaults to False.
        Attributes:
            WS_ID (str): Workspace ID for the Azure Fabric storage.
            METADATA_LH_ID (str): Metadata Lakehouse ID.
            METADATA_TABLE_NAME (str): Name of the metadata table.
            BRONZE_LH_ID (str): Bronze Lakehouse ID.
            SILVER_LH_ID (str): Silver Lakehouse ID.
            CATEGORY (str or None): Category of the ETL process.
            BRONZE_PATH (str): Path to the Bronze Lakehouse.
            SILVER_PATH (str): Path to the Silver Lakehouse.
            METADATA_PATH (str): Path to the Metadata Lakehouse.
            ABSOLUTE_META_TABLE_NAME (str): Absolute path to the metadata table.
            INGESTION_TYPE (str): Type of ingestion process.
            METADATA (pandas.DataFrame): Metadata table filtered based on the configuration.
            allTableList (list): List of all source table names.
            isInitial (bool): Flag indicating whether this is an initial run.
        Raises:
            AssertionError: If 'optionalFilter' is provided and is not a dictionary-like object.
            AssertionError: If the keys of 'optionalFilter' are not a subset of the metadata table columns.
        Example of how to run:
        ```
        config = {
            'WS_ID': 'workspace_id',
            'METADATA_LH_ID': 'metadata_lakehouse_id',
            'METADATA_TABLE_NAME': 'metadata_table_name',
            'BRONZE_LH_ID': 'bronze_lakehouse_id',
            'SILVER_LH_ID': 'silver_lakehouse_id',
            'CATEGORY': 'optional_category',
            'INGESTION_TYPE': 'BronzeToSilver',
            'optionalFilter': {'StatusFlag': True},
            'isInitial': False
        }
        etl = BrToSil_ver2(config)
        infos, errors = etl.execute()
        print("Execution Info:", infos)
        print("Execution Errors:", errors)
        ```
        """

        super().__init__()
        self.WS_ID = config['WS_ID']
        self.METADATA_LH_ID = config['METADATA_LH_ID']
        self.METADATA_TABLE_NAME = config['METADATA_TABLE_NAME']
        self.BRONZE_LH_ID = config['BRONZE_LH_ID']
        self.SILVER_LH_ID = config['SILVER_LH_ID']
        self.CATEGORY = config.get('CATEGORY', None)
        self.at_date = config.get('at_date', None)

        self.BRONZE_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BRONZE_LH_ID}'
        self.SILVER_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SILVER_LH_ID}'
        self.METADATA_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.METADATA_LH_ID}'
        self.ABSOLUTE_META_TABLE_NAME = f'{self.METADATA_PATH}/Tables/mapper/{self.METADATA_TABLE_NAME}'

        self.INGESTION_TYPE = config.get('INGESTION_TYPE','BronzeToSilver')
        self.METADATA = spark.read.load(self.ABSOLUTE_META_TABLE_NAME).filter(col('StatusFlag')).filter(col('IngestionType')==self.INGESTION_TYPE)#.toPandas()
        # optional filter
        if config.get('optionalFilter', None):
            self.optionalFilter = config.get('optionalFilter', None)
            assert isinstance(self.optionalFilter, dict), 'config["optionalFilter"] should be dictionary like object'
            assert set(self.optionalFilter.keys()).issubset(set(self.METADATA.columns)), f'keys of optionalFilter must be in {set(self.METADATA.columns)}'
            
            filter_condition = None
            for column in self.optionalFilter.keys():
                condition = col(column) == self.optionalFilter[column]
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition = filter_condition & condition
            self.METADATA = self.METADATA.filter(filter_condition)

        self.METADATA = self.METADATA.toPandas()
        self.allTableList = self.getAllSourceTableName()

        self.isInitial = config.get('isInitial', False)

    def get_etl_table(self, sourceTableName, logging=False):
        return etl_by_table(self, sourceTableName, self.BRONZE_PATH, self.SILVER_PATH, logging=logging) # can run execute at this object
    
    def getAllSourceTableName(self):
        source = self.METADATA['SourceTable'].str.split('.',expand=True).iloc[:,-1].to_list()
        targetSchema = self.METADATA['TargetTable'].str.split('.',expand=True).iloc[:,0].to_list()
        target = self.METADATA['TargetTable'].str.split('.',expand=True).iloc[:,-1].to_list()
        assert len(source) == len(target), 'weird'
        return list(zip(source, targetSchema, target))
    
    def execute_sequential(self):
        self.infos = []
        self.error = {}
        for sourceTableName, targetSchema, targetTableName in self.allTableList:
            try:
                ad = Utils.AuditLog_STGT(
                    WS_ID= self.WS_ID,
                    TABLE_NAME_to_check=targetTableName,
                    AUDIT_TABLE_NAME='Audit_Log',
                    LH_ID_to_check= self.SILVER_LH_ID ,
                    LH_ID_audit = self.SILVER_LH_ID,
                    schema_check = targetSchema,
                    schema_audit = 'AUDIT'
                    )
                ad.initialDetail(
                    pipelineName = 'NA', 
                    pipelineId = 'NA',
                    TriggerType = 'NA',
                    TableName = targetTableName,
                    functionName = 'BrToSil_ver2.execute'
                )

                etlObject = self.get_etl_table(sourceTableName)
                info = ad.execute(etlObject.execute) #to run ()
                self.infos.append(info)
            except Exception as e:
                self.error[sourceTableName] = e

        return self.infos, self.error
    
    def execute(self, max_workers=4):
        self.infos = []
        self.error = {}
        self.audit = []

        ad = Utils.AuditLog_STGT(
                WS_ID=self.WS_ID,
                TABLE_NAME_to_check='DUMMY',
                AUDIT_TABLE_NAME='Audit_Log',
                LH_ID_to_check=self.SILVER_LH_ID,
                LH_ID_audit=self.SILVER_LH_ID,
                schema_check='DUMMY',
                schema_audit='AUDIT'
            )

        def process_table(table_info):
            sourceTableName, targetSchema, targetTableName = table_info
            try:
                ad = Utils.AuditLog_STGT(
                    WS_ID=self.WS_ID,
                    TABLE_NAME_to_check=targetTableName,
                    AUDIT_TABLE_NAME='Audit_Log',
                    LH_ID_to_check=self.SILVER_LH_ID,
                    LH_ID_audit=self.SILVER_LH_ID,
                    schema_check=targetSchema,
                    schema_audit='AUDIT'
                )
                ad.initialDetail(
                    pipelineName='NA',
                    pipelineId='NA',
                    TriggerType='NA',
                    TableName=targetTableName,
                    functionName='BrToSil_ver2.execute'
                )
                try:
                    ad.countBefore()
                    etlObject = self.get_etl_table(sourceTableName)
                    # info = ad.execute(etlObject.execute)  # to run ()
                    info = etlObject.execute()
                    ad.countAfter()
                    # ad.endSuccess()
                    ad.log['STATUS_ACTIVITY'] = 'Success'
                    ad.log['ENDTIME'] = str(datetime.now() + timedelta(hours=7))
                    row = {}
                    for key in ad.log:
                        row[key] = [str(ad.log[key])]
                    data_tuples = list(zip(*row.values()))
                    df = spark.createDataFrame(data_tuples, schema=list(row.keys()))
                    print(ad)
                    return (info, None, df)
                except Exception as e:
                    # ad.endFail(errorCode = '-', errorMessage = e)
                    ad.log['STATUS_ACTIVITY'] = 'Fail'
                    ad.log['ERRORCODE'] = '-'
                    ad.log['ERRORMESSAGE'] = e
                    # ad._endAuditLog()
                    ad.log['ENDTIME'] = str(datetime.now() + timedelta(hours=7))
                    row = {}
                    for key in ad.log:
                        row[key] = [str(ad.log[key])]
                    data_tuples = list(zip(*row.values()))
                    df = spark.createDataFrame(data_tuples, schema=list(row.keys()))
                    print(ad)
                    return (None, {sourceTableName: e}, df)
                
            except Exception as e:
                print(sourceTableName, ':', e)
                return (None, {sourceTableName: e}, None)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_table, self.allTableList))

        for info, error, df in results:
            if info is not None:
                self.infos.append(info)
            if error is not None:
                self.error.update(error)
            if df is not None:
                self.audit.append(df)

        if len(self.audit) > 0:
            df = self.audit[0]
            for i in range(1, len(self.audit)):
                df = df.unionByName(self.audit[i])
            df.write.mode('append').save(ad.PATH_TO_AUDIT_TABLE)

        return self.infos, self.error

    
    def post_etl(self):
        """
        Updates the METADATA in self.ABSOLUTE_META_TABLE_NAME with the latest information.
        Only updates the relevant part of the table without overwriting the entire dataset.
        """
        metadata_df = spark.read.load(self.ABSOLUTE_META_TABLE_NAME)

        updated_metadata_df = spark.createDataFrame(pd.DataFrame(self.infos))
        updated_metadata_df = Utils.copySchemaByName(updated_metadata_df, metadata_df)

        join_condition = [
            metadata_df['IngestionID'] == updated_metadata_df['IngestionID']
        ]

        unchanged_metadata_df = metadata_df.join(updated_metadata_df, join_condition, "left_anti")
        final_metadata_df = unchanged_metadata_df.union(updated_metadata_df)

        final_metadata_df.write.mode("overwrite").save(self.ABSOLUTE_META_TABLE_NAME)



# class SilToGold_ver2(ETL_base):

#     def __init__(self,config):
#         """
#         Initializes an ETL object with the provided configuration.
#         Args:
#             config (dict): A dictionary containing the following keys:
#                 - WS_ID (str): Workspace ID for the data lake.
#                 - METADATA_LH_ID (str): Metadata Lakehouse ID.
#                 - METADATA_TABLE_NAME (str): Name of the metadata table.
#                 - SILVER_LH_ID (str): Silver Lakehouse ID.
#                 - GOLD_LH_ID (str): Gold Lakehouse ID.
#                 - CATEGORY (str, optional): Category of the ETL process. Defaults to None.
#                 - INGESTION_TYPE (str, optional): Type of ingestion process. Defaults to 'SilverToGold'.
#                 - optionalFilter (dict, optional): A dictionary specifying additional filter conditions. 
#                   Keys must match column names in the metadata table.
#                 - isInitial (bool, optional): Flag indicating if this is an initial run. Defaults to False.
#         Attributes:
#             WS_ID (str): Workspace ID for the data lake.
#             METADATA_LH_ID (str): Metadata Lakehouse ID.
#             METADATA_TABLE_NAME (str): Name of the metadata table.
#             SILVER_LH_ID (str): Silver Lakehouse ID.
#             GOLD_LH_ID (str): Gold Lakehouse ID.
#             CATEGORY (str): Category of the ETL process.
#             SILVER_PATH (str): Path to the Silver Lakehouse.
#             GOLD_PATH (str): Path to the Gold Lakehouse.
#             METADATA_PATH (str): Path to the Metadata Lakehouse.
#             ABSOLUTE_META_TABLE_NAME (str): Absolute path to the metadata table.
#             INGESTION_TYPE (str): Type of ingestion process.
#             METADATA (pandas.DataFrame): Metadata table filtered based on the configuration.
#             allTableList (list): List of all source table names.
#             isInitial (bool): Flag indicating if this is an initial run.
#         Raises:
#             AssertionError: If `optionalFilter` is not a dictionary or if its keys do not match 
#                             the columns in the metadata table.
#         """
#         super().__init__()
#         self.WS_ID = config['WS_ID']
#         self.METADATA_LH_ID = config['METADATA_LH_ID']
#         self.METADATA_TABLE_NAME = config['METADATA_TABLE_NAME']
#         self.SILVER_LH_ID = config['SILVER_LH_ID']
#         self.GOLD_LH_ID = config['GOLD_LH_ID']
#         self.CATEGORY = config.get('CATEGORY', None)

#         self.SILVER_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SILVER_LH_ID}'
#         self.GOLD_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.GOLD_LH_ID}'
#         self.METADATA_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.METADATA_LH_ID}'
#         self.ABSOLUTE_META_TABLE_NAME = f'{self.METADATA_PATH}/Tables/mapper/{self.METADATA_TABLE_NAME}'

#         self.INGESTION_TYPE = config.get('INGESTION_TYPE','SilverToGold')
#         self.METADATA = spark.read.load(self.ABSOLUTE_META_TABLE_NAME).filter(col('StatusFlag')).filter(col('IngestionType')==self.INGESTION_TYPE)#.toPandas()
#         # optional filter
#         if config.get('optionalFilter', None):
#             self.optionalFilter = config.get('optionalFilter', None)
#             assert isinstance(self.optionalFilter, dict), 'config["optionalFilter"] should be dictionary like object'
#             assert set(self.optionalFilter.keys()).issubset(set(self.METADATA.columns)), f'keys of optionalFilter must be in {set(self.METADATA.columns)}'
            
#             filter_condition = None
#             for column in self.optionalFilter.keys():
#                 condition = col(column) == self.optionalFilter[column]
#                 if filter_condition is None:
#                     filter_condition = condition
#                 else:
#                     filter_condition = filter_condition & condition
#             self.METADATA = self.METADATA.filter(filter_condition)

#         self.METADATA = self.METADATA.toPandas() 
#         self.allTableList = self.getAllSourceTableName()

#         self.isInitial = config.get('isInitial', False)

#     def get_etl_table(self, sourceTableName):
#         return etl_by_table(self,sourceTableName, self.SILVER_PATH, self.GOLD_PATH) # can run execute at this object

#     def getAllSourceTableName(self):
#         # sourceSchema = self.METADATA['SourceTable'].str.split('.',expand=True).iloc[:,0].to_list()
#         source = self.METADATA['SourceTable'].str.split('.',expand=True).iloc[:,-1].to_list()
#         targetSchema = self.METADATA['TargetTable'].str.split('.',expand=True).iloc[:,0].to_list()
#         target = self.METADATA['TargetTable'].str.split('.',expand=True).iloc[:,-1].to_list()
#         assert len(source) == len(target), 'weird'
#         # return list(zip(sourceSchema,source, targetSchema, target))
#         return list(zip(source, targetSchema, target))

#     def execute_sequential(self):
#         self.infos = []
#         self.error = {}
#         for sourceSchema,sourceTableName, targetSchema, targetTableName in self.allTableList:
#             try:
#                 self.ad = Utils.AuditLog_STGT(
#                     WS_ID= self.WS_ID,
#                     TABLE_NAME_to_check=targetTableName,
#                     AUDIT_TABLE_NAME='Audit_Log',
#                     LH_ID_to_check= self.GOLD_LH_ID ,
#                     LH_ID_audit = self.SILVER_LH_ID,
#                     schema_check = targetSchema,
#                     schema_audit = 'AUDIT'
#                     )
#                 self.ad.initialDetail(
#                     pipelineName = 'NA', 
#                     pipelineId = 'NA',
#                     TriggerType = 'NA',
#                     TableName = targetTableName,
#                     functionName = 'SilToGold_ver2.execute'
#                 )

#                 etlObject = self.get_etl_table(sourceTableName)
#                 info = self.ad.execute(etlObject.execute) #to run ()
#                 self.infos.append(info)
#             except Exception as e:
#                 self.error[sourceTableName] = e
#         return self.infos, self.error
#         '''
#         with ThreadPoolExecutor(max_workers = self.max_workers) as p:
#             results = list(p.map(self.runQuerySpark_TableName,allTable))
#         '''

#     def execute(self, max_workers=4):
#         self.infos = []
#         self.error = {}
#         self.audit = []

#         ad = Utils.AuditLog_STGT(
#                 WS_ID=self.WS_ID,
#                 TABLE_NAME_to_check='DUMMY',
#                 AUDIT_TABLE_NAME='Audit_Log',
#                 LH_ID_to_check=self.GOLD_LH_ID,
#                 LH_ID_audit=self.SILVER_LH_ID,
#                 schema_check='DUMMY',
#                 schema_audit='AUDIT'
#             )

#         def process_table(table_info):
#             sourceTableName, targetSchema, targetTableName = table_info
#             try:
#                 ad = Utils.AuditLog_STGT(
#                     WS_ID=self.WS_ID,
#                     TABLE_NAME_to_check=targetTableName,
#                     AUDIT_TABLE_NAME='Audit_Log',
#                     LH_ID_to_check=self.GOLD_LH_ID,
#                     LH_ID_audit=self.SILVER_LH_ID,
#                     schema_check=targetSchema,
#                     schema_audit='AUDIT'
#                 )
#                 ad.initialDetail(
#                     pipelineName='NA',
#                     pipelineId='NA',
#                     TriggerType='NA',
#                     TableName=targetTableName,
#                     functionName='SilToGold_ver2.execute'
#                 )
#                 try:
#                     ad.countBefore()
#                     etlObject = self.get_etl_table(sourceTableName)
#                     # info = ad.execute(etlObject.execute)  # to run ()
#                     info = etlObject.execute()
#                     ad.countAfter()
#                     # ad.endSuccess()
#                     ad.log['STATUS_ACTIVITY'] = 'Success'
#                     ad.log['ENDTIME'] = str(datetime.now() + timedelta(hours=7))
#                     row = {}
#                     for key in ad.log:
#                         row[key] = [str(ad.log[key])]
#                     data_tuples = list(zip(*row.values()))
#                     df = spark.createDataFrame(data_tuples, schema=list(row.keys()))
#                     print(ad)
#                     return (info, None, df)
#                 except Exception as e:
#                     # ad.endFail(errorCode = '-', errorMessage = e)
#                     ad.log['STATUS_ACTIVITY'] = 'Fail'
#                     ad.log['ERRORCODE'] = '-'
#                     ad.log['ERRORMESSAGE'] = e
#                     # ad._endAuditLog()
#                     ad.log['ENDTIME'] = str(datetime.now() + timedelta(hours=7))
#                     row = {}
#                     for key in ad.log:
#                         row[key] = [str(ad.log[key])]
#                     data_tuples = list(zip(*row.values()))
#                     df = spark.createDataFrame(data_tuples, schema=list(row.keys()))
#                     print(ad)
#                     return (None, {sourceTableName: e}, df)

#             except Exception as e:
#                 print(sourceTableName, ':', e)
#                 return (None, {sourceTableName: e}, None)

#         with ThreadPoolExecutor(max_workers=max_workers) as executor:
#             results = list(executor.map(process_table, self.allTableList))

#         for info, error, df in results:
#             if info is not None:
#                 self.infos.append(info)
#             if error is not None:
#                 self.error.update(error)
#             if df is not None:
#                 self.audit.append(df)

#         if len(self.audit) > 0:
#             df = self.audit[0]
#             for i in range(1, len(self.audit)):
#                 df = df.unionByName(self.audit[i])
#             df.write.mode('append').save(ad.PATH_TO_AUDIT_TABLE)

#         return self.infos, self.error
    
#     def post_etl(self):
#         """
#         Updates the METADATA in self.ABSOLUTE_META_TABLE_NAME with the latest information.
#         Only updates the relevant part of the table without overwriting the entire dataset.
#         """
#         metadata_df = spark.read.load(self.ABSOLUTE_META_TABLE_NAME)

#         updated_metadata_df = spark.createDataFrame(pd.DataFrame(self.infos))
#         updated_metadata_df = Utils.copySchemaByName(updated_metadata_df, metadata_df)

#         join_condition = [
#             metadata_df['IngestionID'] == updated_metadata_df['IngestionID']
#         ]

#         unchanged_metadata_df = metadata_df.join(updated_metadata_df, join_condition, "left_anti")
#         final_metadata_df = unchanged_metadata_df.union(updated_metadata_df)

#         final_metadata_df.write.mode("overwrite").save(self.ABSOLUTE_META_TABLE_NAME)
class SilToGold_ver2(ETL_base):

    def __init__(self,config):
        """
        Initializes an ETL object with the provided configuration.
        Args:
            config (dict): A dictionary containing the following keys:
                - WS_ID (str): Workspace ID for the data lake.
                - METADATA_LH_ID (str): Metadata Lakehouse ID.
                - METADATA_TABLE_NAME (str): Name of the metadata table.
                - SILVER_LH_ID (str): Silver Lakehouse ID.
                - GOLD_LH_ID (str): Gold Lakehouse ID.
                - CATEGORY (str, optional): Category of the ETL process. Defaults to None.
                - INGESTION_TYPE (str, optional): Type of ingestion process. Defaults to 'SilverToGold'.
                - optionalFilter (dict, optional): A dictionary specifying additional filter conditions. 
                  Keys must match column names in the metadata table.
                - isInitial (bool, optional): Flag indicating if this is an initial run. Defaults to False.
        Attributes:
            WS_ID (str): Workspace ID for the data lake.
            METADATA_LH_ID (str): Metadata Lakehouse ID.
            METADATA_TABLE_NAME (str): Name of the metadata table.
            SILVER_LH_ID (str): Silver Lakehouse ID.
            GOLD_LH_ID (str): Gold Lakehouse ID.
            CATEGORY (str): Category of the ETL process.
            SILVER_PATH (str): Path to the Silver Lakehouse.
            GOLD_PATH (str): Path to the Gold Lakehouse.
            METADATA_PATH (str): Path to the Metadata Lakehouse.
            ABSOLUTE_META_TABLE_NAME (str): Absolute path to the metadata table.
            INGESTION_TYPE (str): Type of ingestion process.
            METADATA (pandas.DataFrame): Metadata table filtered based on the configuration.
            allTableList (list): List of all source table names.
            isInitial (bool): Flag indicating if this is an initial run.
        Raises:
            AssertionError: If `optionalFilter` is not a dictionary or if its keys do not match 
                            the columns in the metadata table.
        """
        super().__init__()
        self.WS_ID = config['WS_ID']
        self.METADATA_LH_ID = config['METADATA_LH_ID']
        self.METADATA_TABLE_NAME = config['METADATA_TABLE_NAME']
        self.SILVER_LH_ID = config['SILVER_LH_ID']
        self.GOLD_LH_ID = config['GOLD_LH_ID']
        self.CATEGORY = config.get('CATEGORY', None)
        self.at_date = config.get('at_date', None)

        self.SILVER_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SILVER_LH_ID}'
        self.GOLD_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.GOLD_LH_ID}'
        self.METADATA_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.METADATA_LH_ID}'
        self.ABSOLUTE_META_TABLE_NAME = f'{self.METADATA_PATH}/Tables/mapper/{self.METADATA_TABLE_NAME}'

        self.INGESTION_TYPE = config.get('INGESTION_TYPE','SilverToGold')
        self.METADATA = spark.read.load(self.ABSOLUTE_META_TABLE_NAME).filter(col('StatusFlag')).filter(col('IngestionType')==self.INGESTION_TYPE)

        # all etl in silver to gold is scdFlag = 1 (already scd type 2)
        # self.METADATA = self.METADATA.withColumn('SourceSCD2Flag', lit(1))
        
        # optional filter
        if config.get('optionalFilter', None):
            self.optionalFilter = config.get('optionalFilter', None)
            assert isinstance(self.optionalFilter, dict), 'config["optionalFilter"] should be dictionary like object'
            assert set(self.optionalFilter.keys()).issubset(set(self.METADATA.columns)), f'keys of optionalFilter must be in {set(self.METADATA.columns)}'
            
            filter_condition = None
            for column in self.optionalFilter.keys():
                condition = col(column) == self.optionalFilter[column]
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition = filter_condition & condition
            self.METADATA = self.METADATA.filter(filter_condition)

        self.METADATA = self.METADATA.toPandas() 
        self.allTableList = self.getAllSourceTableName()

        self.isInitial = config.get('isInitial', False)

    def get_etl_table(self, sourceTableName, logging=False):
        return etl_by_table(self, sourceTableName, self.SILVER_PATH, self.GOLD_PATH, logging=logging) # can run execute at this object
    
    def getAllSourceTableName(self):
        source = self.METADATA['SourceTable'].str.split('.',expand=True).iloc[:,-1].to_list()
        targetSchema = self.METADATA['TargetTable'].str.split('.',expand=True).iloc[:,0].to_list()
        target = self.METADATA['TargetTable'].str.split('.',expand=True).iloc[:,-1].to_list()
        assert len(source) == len(target), 'weird'
        return list(zip(source, targetSchema, target))

    def execute_sequential(self):
        self.infos = []
        self.error = {}
        for sourceTableName, targetSchema, targetTableName in self.allTableList:
            try:
                self.ad = Utils.AuditLog_STGT(
                    WS_ID= self.WS_ID,
                    TABLE_NAME_to_check=targetTableName,
                    AUDIT_TABLE_NAME='Audit_Log',
                    LH_ID_to_check= self.GOLD_LH_ID ,
                    LH_ID_audit = self.SILVER_LH_ID,
                    schema_check = targetSchema,
                    schema_audit = 'AUDIT'
                    )
                self.ad.initialDetail(
                    pipelineName = 'NA', 
                    pipelineId = 'NA',
                    TriggerType = 'NA',
                    TableName = targetTableName,
                    functionName = 'SilToGold_ver2.execute'
                )

                etlObject = self.get_etl_table(sourceTableName)
                info = self.ad.execute(etlObject.execute) #to run ()
                self.infos.append(info)
            except Exception as e:
                self.error[sourceTableName] = e

        return self.infos, self.error

    def execute(self, max_workers=4):
        self.infos = []
        self.error = {}
        self.audit = []

        ad = Utils.AuditLog_STGT(
                WS_ID=self.WS_ID,
                TABLE_NAME_to_check='DUMMY',
                AUDIT_TABLE_NAME='Audit_Log',
                LH_ID_to_check=self.GOLD_LH_ID,
                LH_ID_audit=self.SILVER_LH_ID,
                schema_check='DUMMY',
                schema_audit='AUDIT'
            )

        def process_table(table_info):
            sourceTableName, targetSchema, targetTableName = table_info
            try:
                ad = Utils.AuditLog_STGT(
                    WS_ID=self.WS_ID,
                    TABLE_NAME_to_check=targetTableName,
                    AUDIT_TABLE_NAME='Audit_Log',
                    LH_ID_to_check=self.GOLD_LH_ID,
                    LH_ID_audit=self.SILVER_LH_ID,
                    schema_check=targetSchema,
                    schema_audit='AUDIT'
                )
                ad.initialDetail(
                    pipelineName='NA',
                    pipelineId='NA',
                    TriggerType='NA',
                    TableName=targetTableName,
                    functionName='SilToGold_ver2.execute'
                )
                try:
                    ad.countBefore()
                    etlObject = self.get_etl_table(sourceTableName)
                    # info = ad.execute(etlObject.execute)  # to run ()
                    info = etlObject.execute()
                    ad.countAfter()
                    # ad.endSuccess()
                    ad.log['STATUS_ACTIVITY'] = 'Success'
                    ad.log['ENDTIME'] = str(datetime.now() + timedelta(hours=7))
                    row = {}
                    for key in ad.log:
                        row[key] = [str(ad.log[key])]
                    data_tuples = list(zip(*row.values()))
                    df = spark.createDataFrame(data_tuples, schema=list(row.keys()))
                    print(ad)
                    return (info, None, df)
                except Exception as e:
                    # ad.endFail(errorCode = '-', errorMessage = e)
                    ad.log['STATUS_ACTIVITY'] = 'Fail'
                    ad.log['ERRORCODE'] = '-'
                    ad.log['ERRORMESSAGE'] = e
                    # ad._endAuditLog()
                    ad.log['ENDTIME'] = str(datetime.now() + timedelta(hours=7))
                    row = {}
                    for key in ad.log:
                        row[key] = [str(ad.log[key])]
                    data_tuples = list(zip(*row.values()))
                    df = spark.createDataFrame(data_tuples, schema=list(row.keys()))
                    print(ad)
                    return (None, {sourceTableName: e}, df)

            except Exception as e:
                print(sourceTableName, ':', e)
                return (None, {sourceTableName: e}, None)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_table, self.allTableList))

        for info, error, df in results:
            if info is not None:
                self.infos.append(info)
            if error is not None:
                self.error.update(error)
            if df is not None:
                self.audit.append(df)

        if len(self.audit) > 0:
            df = self.audit[0]
            for i in range(1, len(self.audit)):
                df = df.unionByName(self.audit[i])
            df.write.mode('append').save(ad.PATH_TO_AUDIT_TABLE)

        return self.infos, self.error
    
    def post_etl(self):
        """
        Updates the METADATA in self.ABSOLUTE_META_TABLE_NAME with the latest information.
        Only updates the relevant part of the table without overwriting the entire dataset.
        """
        metadata_df = spark.read.load(self.ABSOLUTE_META_TABLE_NAME)

        updated_metadata_df = spark.createDataFrame(pd.DataFrame(self.infos))
        updated_metadata_df = Utils.copySchemaByName(updated_metadata_df, metadata_df)

        join_condition = [
            metadata_df['IngestionID'] == updated_metadata_df['IngestionID']
        ]

        unchanged_metadata_df = metadata_df.join(updated_metadata_df, join_condition, "left_anti")
        final_metadata_df = unchanged_metadata_df.union(updated_metadata_df)

        final_metadata_df.write.mode("overwrite").save(self.ABSOLUTE_META_TABLE_NAME)



class etl_by_table(ETL_base):
    '''
    todo task
    [ ] Implement incremental load
    [ ] Implement scd type2 in save
    '''

    def __init__(self, b2s: BrToSil_ver2, sourceTableName: str, sourcePath: str, targetPath: str, logging =False):
        self.BrToSil_object = b2s
        self.METADATA = self.BrToSil_object.METADATA
        self.tableInfo = self.METADATA[self.METADATA['SourceTable'].str.split('.',expand=True).iloc[:,-1] == sourceTableName].iloc[0]
        self.sourceSchema, self.sourceTableName = self.tableInfo.loc['SourceTable'].split(".")
        self.targetSchema, self.targetTableName = self.tableInfo.loc['TargetTable'].split(".")
        if self.tableInfo['DQRule']:
            self.DQRule = ast.literal_eval(self.tableInfo['DQRule'])
        else:
            self.DQRule = {column:'0,0,0,0,0,0,0,0' for column in ast.literal_eval(self.tableInfo['TargetColumn'])}
        self.DQRule = {key: [int(x) for x in self.DQRule[key].split(",")] for key in self.DQRule}
        self.columnMapper = self.getColumnMapper()
        self.SOURCE_PATH = f'{sourcePath}/Tables/{self.sourceSchema}/{self.sourceTableName}'
        self.STAGING_PATH = f'{sourcePath}/Tables/staging/staging_{self.tableInfo.loc["DataDomain"]}_{self.sourceTableName}_{self.targetTableName}'
        self.TARGET_PATH = f'{targetPath}/Tables/{self.targetSchema}/{self.targetTableName}'
        self.TableType = self.tableInfo.loc['TableType']
        self.PrimaryKeyColumn = ast.literal_eval(self.tableInfo.loc['PrimaryKeyColumn'])
        self.current_ict_time = datetime.now(pytz.utc).astimezone(ict_timezone)
        if self.tableInfo.loc['PartitionColumn']:
            self.tableInfo.loc['PartitionColumn'] = None if self.tableInfo.loc['PartitionColumn'].strip() == '' else self.tableInfo.loc['PartitionColumn']
        self.PartitionColumn = ast.literal_eval(self.tableInfo.loc['PartitionColumn']) if self.tableInfo.loc['PartitionColumn'] else None
        self.LastRunTimestamp = self.tableInfo.loc['LastRunTimestamp']
        self.logging = logging
        if logging:
            self.log = {}
        
    def getColumnMapper(self):
        sourceType = ast.literal_eval(self.tableInfo.loc['SourceType'])
        sourceType = [re.findall(r'([a-zA-Z]+|[0-9]+)',x) for x in sourceType]
        
        newSourceType = []
        for x in sourceType:
            if len(x) == 1:
                newSourceType.append(x+['0','0'])
            elif len(x) == 2:
                newSourceType.append(x+['0'])
            else:
                newSourceType.append(x)
        sourceType = newSourceType

        sourceTypeDf = pd.DataFrame(sourceType,columns=['type_source','precision_source','scale_source'])
        sourceTypeDf['type_source_original'] = sourceTypeDf['type_source']
        sourceTypeDf['type_source'] = sourceTypeDf['type_source'].map(self.BrToSil_object.TYPE_MAPPER)
        sourceTypeDf['precision_source'] = sourceTypeDf['precision_source'].fillna(0).astype(int)
        sourceTypeDf['scale_source'] = sourceTypeDf['scale_source'].fillna(0).astype(int)
        sourceTypeDf['column_source'] = ast.literal_eval(self.tableInfo.loc['SourceColumn'])

        targetType = ast.literal_eval(self.tableInfo.loc['TargetType'])
        targetType = [re.findall(r'([a-zA-Z]+|[0-9]+)',x) for x in targetType]

        newTargetType = []
        for x in targetType:
            if len(x) == 1:
                newTargetType.append(x+['0','0'])
            elif len(x) == 2:
                newTargetType.append(x+['0'])
            else:
                newTargetType.append(x)
        targetType = newTargetType

        targetTypeDf = pd.DataFrame(targetType,columns=['type_target','precision_target','scale_target'])
        targetTypeDf['type_target_original'] = targetTypeDf['type_target']
        targetTypeDf['type_target'] = targetTypeDf['type_target'].map(self.BrToSil_object.TYPE_MAPPER)
        targetTypeDf['precision_target'] = targetTypeDf['precision_target'].fillna(0).astype(int)
        targetTypeDf['scale_target'] = targetTypeDf['scale_target'].fillna(0).astype(int)
        targetTypeDf['column_target'] = ast.literal_eval(self.tableInfo.loc['TargetColumn'])

        columnMap = pd.concat([sourceTypeDf,targetTypeDf], axis=1).set_index('column_target')
        
        DQRule_df = pd.DataFrame(self.DQRule).transpose()
        DQRule_df.columns = [f'DQ{int(c)+1}' for c in DQRule_df.columns]
        return columnMap.join(DQRule_df)

    def load_latest(self, df, date_ref_column, threshold, selected_column,):
        df_latest = df.filter(col(date_ref_column)>= threshold).select(selected_column).cache()
        return df_latest
    
    def load_at_date(self, df, date_ref_column, at_date, selected_column):
        """
        Filters a DataFrame to include only rows where the specified date reference column matches a given date,
        and selects specific columns from the filtered DataFrame.

        Args:
            df (pyspark.sql.DataFrame): The input DataFrame to filter and select data from.
            date_ref_column (str): The name of the column in the DataFrame containing date values to filter by.
            date (str): The target date in "yyyyMMdd" format to filter rows.
            selected_column (str or list): The column(s) to select from the filtered DataFrame.

        Returns:
            pyspark.sql.DataFrame: A cached DataFrame containing the selected columns from rows that match the given date.
        """
        df_latest = df.filter(date_format(col(date_ref_column), "yyyyMMdd") <= at_date).select(selected_column).cache()
        return df_latest

    def loadSource(self):
        '''
        Load from bronze: filter only the latest records for master table and load the entire table for transaction
        save on staging and reload from table (for lazy evaluation manner)
        Finally, `self.sourceTable` is the table loaded from staging
        '''
        selectedColumns = ast.literal_eval(self.tableInfo.loc['SourceColumn'])
        bronzeTable = spark.read.load(self.SOURCE_PATH)
        if self.TableType.lower() == 'master':
            '''
            load only the lates one
            '''
            last_run_timestamp = pd.to_datetime('1900-01-01') if pd.isna(self.LastRunTimestamp) else ast.literal_eval(self.LastRunTimestamp)
            if self.BrToSil_object.isInitial:
                latest_bronze = bronzeTable.select(selectedColumns)
            elif self.BrToSil_object.at_date:
                latest_bronze = self.load_at_date(df = bronzeTable, date_ref_column='TimeStamp', date=self.BrToSil_object.at_date, selected_column=selectedColumns)
            else:
                latest_bronze = self.load_latest(df = bronzeTable, date_ref_column='TimeStamp', threshold=last_run_timestamp, selected_column=selectedColumns)
            if self.logging:
                self.log['sourceTable_rows'] = latest_bronze.count()
                
            self.sourceTable = self.reload(latest_bronze, self.STAGING_PATH).select(selectedColumns)   
            return self.sourceTable
        elif self.TableType.lower() == 'transaction':
            '''
            Load all
            '''
            bronzeTable = bronzeTable.select(selectedColumns)
            if self.logging:
                self.log['sourceTable_rows'] = bronzeTable.count()
            self.sourceTable = self.reload(bronzeTable, self.STAGING_PATH).select(selectedColumns)
            return self.sourceTable
    
    # def loadSource_old(self):
    #     selectedColumns = ast.literal_eval(self.tableInfo.loc['SourceColumn'])
        
    #     if 'full' in self.tableInfo.loc['LoadType'].lower():
    #         self.sourceTable = spark.read.load(self.SOURCE_PATH).select(selectedColumns)
    #     elif self.tableInfo.loc['LoadType'].lower() == 'Incremental'.lower():
    #         if self.BrToSil_object.isInitial:
    #             self.sourceTable = spark.read.load(self.SOURCE_PATH).select(selectedColumns)
    #         else:
    #             '''
    #             use self.LastRunTimestamp to filter
    #             '''
    #             if self.BrToSil_object.INGESTION_TYPE == 'BronzeToSilver':
    #                 assert self.tableInfo.loc['IncrementalColumn'] is not None, "no IncrementalColumn is specified"
                
    #             incremental_columns = ast.literal_eval(self.tableInfo.loc['IncrementalColumn']) if self.tableInfo.loc['IncrementalColumn'] else []
    #             lower_bound = pd.to_datetime('1970-01-01') if pd.isna(self.tableInfo.loc['LastRunTimestamp']) else ast.literal_eval(self.tableInfo.loc['LastRunTimestamp'])
    #             filter_condition = None
    #             for column in incremental_columns:
    #                 source_col = self.columnMapper.loc[column,'column_source']
    #                 if self.columnMapper.loc[column, 'type_source_original'] == "decimal":
    #                     condition = to_timestamp(col(source_col).cast(LongType()).cast(StringType()), "yyyyMMddHHmmss") > lower_bound
    #                 elif self.columnMapper.loc[column, 'type_source_original'] == "varchar":
    #                     condition = to_timestamp(col(source_col), "yyyyMMdd") > lower_bound
    #                 else:
    #                     condition = col(source_col) > lower_bound
    #                 if filter_condition is None:
    #                     filter_condition = condition
    #                 else:
    #                     filter_condition = filter_condition | condition
    #             if filter_condition is not None:
    #                 self.sourceTable = spark.read.load(self.SOURCE_PATH).filter(filter_condition).select(selectedColumns)
    #             else:
    #                 self.sourceTable = spark.read.load(self.SOURCE_PATH).select(selectedColumns)
    #     else:
    #         raise NameError("wrong load type")
    #     if self.logging:
    #         self.log['sourceTable_rows'] = self.sourceTable.count()
    #     return self.sourceTable
    
    def changeColumnName(self):
        changeNameMapper = dict(self.columnMapper.reset_index().set_index('column_source')['column_target'])
        self.sourceTable = self.changeName(self.sourceTable,changeNameMapper)
        if self.logging:
            self.log['changeColumnName'] = self.sourceTable.count()
    
    def applyType(self):
        typeSource = dict(self.columnMapper['type_source'])
        typeMapper = dict(self.columnMapper['type_target'])
        precisionMapper = dict(self.columnMapper['precision_target'])
        scaleMapper = dict(self.columnMapper['scale_target'])
        self.sourceTable = self.changeType(self.sourceTable, typeSource, typeMapper, precisionMapper, scaleMapper)
        #                       changeType(    df          , typeSource, typeMapper, precisionMapper, scaleMapper)
        if self.logging:
            self.log['applyType'] = self.sourceTable.count()

    def DQ1(self):
        columns_dq1 = list(self.columnMapper[self.columnMapper['DQ1']==1].index)
        self.sourceTable = self.fillNa(self.sourceTable, columns_dq1)
        if self.logging:
            self.log['DQ1'] = self.sourceTable.count()
    def DQ2(self):
        columns_dq2 = list(self.columnMapper[self.columnMapper['DQ2']==1].index)
        pass
    def DQ3(self):
        columns_dq3 = list(self.columnMapper[self.columnMapper['DQ3']==1].index)
        pass
    def DQ4(self):
        columns_dq4 = list(self.columnMapper[self.columnMapper['DQ4']==1].index)
        pass
    def DQ5(self):
        columns_dq5 = list(self.columnMapper[self.columnMapper['DQ5']==1].index)
        pass
    def DQ6(self):
        columns_dq6 = list(self.columnMapper[self.columnMapper['DQ6']==1].index)
        pass
    def DQ7(self):
        # if self.INGESTION_TYPE == 'SilverToGold':
        #     return  # Skip DQ7 logic
        columns_dq7 = list(self.columnMapper[self.columnMapper['DQ7']==1].index)
        #self.sourceTable = self.sourceTable.withColumns({column: when(col(column)=='',to_timestamp(lit('19700101000000'),"yyyyMMddHHmmss")).otherwise(to_timestamp(concat(lit('19700101'), col(column)),"yyyyMMddHHmmss")) for column in columns_dq7})
        #self.sourceTable = self.sourceTable.withColumns({column: when(col(column) == '', to_timestamp(lit('19700101000000'), "yyyyMMddHHmmss")).when(length(col(column)) == 5,to_timestamp(concat(lit('19700101'),substring(col(column), 1, 2), substring(col(column), 4, 2), lit('00')), "yyyyMMddHHmmss")).otherwise(to_timestamp(concat(lit('19700101'), col(column), lit('00')), "yyyyMMddHHmmss")) for column in columns_dq7})
        self.sourceTable = self.sourceTable.withColumns(
                {column: 
                    when(
                        col(column) == '',
                        to_timestamp(lit('19700101000000'), "yyyyMMddHHmmss")
                    ).otherwise(
                        when(
                            col(column).contains(":"), #HH:mm
                            to_timestamp(concat(lit('19700101'),substring(col(column), 1, 2), substring(col(column), 4, 2), lit('00')), "yyyyMMddHHmmss")
                        ).otherwise( # HHmmss
                                to_timestamp(concat(lit('19700101'), col(column)), "yyyyMMddHHmmss")
                                )
                            )  
                for column in columns_dq7
                }
            )
    # def DQ7(self):
    #     # if self.INGESTION_TYPE == 'SilverToGold':
    #     #     return  # Skip DQ7 logic
    #     columns_dq7 = list(self.columnMapper[self.columnMapper['DQ7']==1].index)
    #     #self.sourceTable = self.sourceTable.withColumns({column: when(col(column)=='',to_timestamp(lit('19700101000000'),"yyyyMMddHHmmss")).otherwise(to_timestamp(concat(lit('19700101'), col(column)),"yyyyMMddHHmmss")) for column in columns_dq7})
    #     #self.sourceTable = self.sourceTable.withColumns({column: when(col(column) == '', to_timestamp(lit('19700101000000'), "yyyyMMddHHmmss")).when(length(col(column)) == 5,to_timestamp(concat(lit('19700101'),substring(col(column), 1, 2), substring(col(column), 4, 2), lit('00')), "yyyyMMddHHmmss")).otherwise(to_timestamp(concat(lit('19700101'), col(column), lit('00')), "yyyyMMddHHmmss")) for column in columns_dq7})
    #     self.sourceTable = self.sourceTable.withColumns(
    #             {column: 
    #                 when(
    #                     col(column) == '',
    #                     to_timestamp(lit('19700101000000'), "yyyyMMddHHmmss")
    #                 ).otherwise(
    #                     when(
    #                         col(column).contains(":"), #HH:mm
    #                         to_timestamp(concat(lit('19700101'),substring(col(column), 1, 2), substring(col(column), 4, 2), lit('00')), "yyyyMMddHHmmss")
    #                     ).otherwise( # HHmmss
    #                             to_timestamp(concat(lit('19700101'), col(column)), "yyyyMMddHHmmss")
    #                             )
    #                         )  
    #             for column in columns_dq7
    #             }
    #         )
    def DQ8(self):
        columns_dq8 = list(self.columnMapper[self.columnMapper['DQ8']==1].index)
        pass
        # self.sourceTable = self.sourceTable.withColumns({column: when(upper(trim(col(column))) == 'X', True).otherwise(False) for column in columns_dq8})
        # if self.logging:
        #     self.log['DQ8'] = self.sourceTable.count()
    #def DQ8(self):
    #    columns_dq8 = list(self.columnMapper[self.columnMapper['DQ8']==1].index)
    #    self.sourceTable = self.sourceTable.withColumns({column: when(col(column)=='X', True).otherwise(False) for column in columns_dq8})
    #    if self.logging:
    #        self.log['DQ8'] = self.sourceTable.count()

    # def save_initial(self):
    #     if self.TableType.lower() == 'master':
    #         if self.tableInfo.loc['SourceSCD2Flag']:
    #             self.sourceTable = self.sourceTable.withColumn('startDate',current_date()).withColumn('endDate',to_date(lit('9999-12-31'))).withColumn('activeFlag',lit(True))
    #             if self.tableInfo.loc['PartitionColumn']:
    #                 if self.logging:
    #                     self.log['save_initial'] = self.sourceTable.count()
    #                 self.sourceTable.write.mode('overwrite').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).option('overwriteSchema','true').save(self.TARGET_PATH)
    #             else:
    #                 if self.logging:
    #                     self.log['save_initial'] = self.sourceTable.count()
    #                 self.sourceTable.write.mode('overwrite').option('overwriteSchema','true').save(self.TARGET_PATH)
    #         else:
    #             if self.tableInfo.loc['PartitionColumn']:
    #                 if self.logging:
    #                     self.log['save_initial'] = self.sourceTable.count()
    #                 self.sourceTable.write.mode('overwrite').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).option('overwriteSchema','true').save(self.TARGET_PATH)
    #             else:
    #                 if self.logging:
    #                     self.log['save_initial'] = self.sourceTable.count()
    #                 self.sourceTable.write.mode('overwrite').option('overwriteSchema','true').save(self.TARGET_PATH)

    #     elif self.TableType.lower() == 'transaction': # always append
    #         if self.tableInfo.loc['PartitionColumn']:
    #             # self.sourceTable.write.mode('append').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).save(self.TARGET_PATH) # TODO: when actual run
    #             if self.logging:
    #                 self.log['save_initial'] = self.sourceTable.count()
    #             self.sourceTable.write.mode('overwrite').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).option('overwriteSchema','true').save(self.TARGET_PATH)
    #         else:
    #             # self.sourceTable.write.mode('append').save(self.TARGET_PATH)
    #             if self.logging:
    #                 self.log['save_initial'] = self.sourceTable.count()
    #             self.sourceTable.write.mode('overwrite').option('overwriteSchema','true').save(self.TARGET_PATH)

    #     else:
    #         raise NameError("wrong table type")
    def save_initial(self):
        if self.TableType.lower() == 'master':
            if self.tableInfo.loc['SourceSCD2Flag']:
                # self.sourceTable = self.sourceTable.withColumn('startDate',current_date()).withColumn('endDate',to_date(lit('9999-12-31'))).withColumn('activeFlag',lit(True))
                if self.tableInfo.loc['PartitionColumn']:
                    if self.logging:
                        self.log['save_initial'] = self.sourceTable.count()
                    self.sourceTable.write.mode('overwrite').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).option('overwriteSchema','true').save(self.TARGET_PATH)
                else:
                    if self.logging:
                        self.log['save_initial'] = self.sourceTable.count()
                    self.sourceTable.write.mode('overwrite').option('overwriteSchema','true').save(self.TARGET_PATH)
            else:
                if self.tableInfo.loc['PartitionColumn']:
                    if self.logging:
                        self.log['save_initial'] = self.sourceTable.count()
                    self.sourceTable = self.sourceTable.withColumn('startDate',current_date()).withColumn('endDate',to_date(lit('9999-12-31'))).withColumn('activeFlag',lit(True))
                    self.sourceTable.write.mode('overwrite').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).option('overwriteSchema','true').save(self.TARGET_PATH)
                else:
                    if self.logging:
                        self.log['save_initial'] = self.sourceTable.count()
                    self.sourceTable = self.sourceTable.withColumn('startDate',current_date()).withColumn('endDate',to_date(lit('9999-12-31'))).withColumn('activeFlag',lit(True))
                    self.sourceTable.write.mode('overwrite').option('overwriteSchema','true').save(self.TARGET_PATH)

        elif self.TableType.lower() == 'transaction': # always append
            if self.tableInfo.loc['PartitionColumn']:
                # self.sourceTable.write.mode('append').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).save(self.TARGET_PATH) # TODO: when actual run
                if self.logging:
                    self.log['save_initial'] = self.sourceTable.count()
                self.sourceTable.write.mode('overwrite').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).option('overwriteSchema','true').save(self.TARGET_PATH)
            else:
                # self.sourceTable.write.mode('append').save(self.TARGET_PATH)
                if self.logging:
                    self.log['save_initial'] = self.sourceTable.count()
                self.sourceTable.write.mode('overwrite').option('overwriteSchema','true').save(self.TARGET_PATH)

        else:
            raise NameError("wrong table type")

    def save_transaction(self, sourceTable, targetPath, primarykey, partition):
        targetTable = spark.read.load(targetPath)
        left_anti = targetTable.join(sourceTable, on=primarykey, how='left_anti')
        newTransaction = left_anti.unionByName(sourceTable, allowMissingColumns=True)
        writing = newTransaction.write.mode('overwrite')
        if partition:
           writing = writing.partitionBy(partition)
        writing.save(targetPath)
        return spark.read.load(targetPath)
    
    def save_master(self, sourceTable, targetPath, primarykey, partition, scd2flag):
        if scd2flag: # = 1
            '''
            already scdtype 2 just overwrite the whole table
            '''
            writing = sourceTable.write.mode('overwrite')
            if partition:
                writing = writing.partitionBy(partition)
            writing.save(targetPath)
        else:
            '''
            do scdtype 2 by itself
            '''
            assert primarykey is not None, "no primary key is specified"

            if isinstance(primarykey, str):
                primarykey = [primarykey]

            targetTable = spark.read.load(targetPath)

            comparedColumns = [column for column in targetTable.columns if column not in primarykey + ['TimeStamp', 'AuditTimestamp', 'startDate', 'endDate', 'activeFlag','ModifiedDate']]
            final = self.scdType2(sourceTable=sourceTable, targetTable=targetTable, primarykey=primarykey, comparedColumns=comparedColumns)
            writing = final.write.mode('overwrite')
            if partition:
                writing = writing.partitionBy(partition)
            writing.save(targetPath)

    # def save(self):
    #     if self.TableType.lower() == 'master':
    #         if 1 - self.tableInfo.loc['SourceSCD2Flag']:
    #             # [x] TODO: Implement scd type 2 later -- 20250405
    #             if self.tableInfo.loc['PartitionColumn']:
    #                 self.scdType2(source=self.sourceTable, target=spark.read.load(self.TARGET_PATH), on=self.PrimaryKeyColumn, startDate= 'startDate', endDate='endDate', activeFlag='activeFlag')\
    #                     .write.mode('overwrite').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).save(self.TARGET_PATH)
    #             else:
    #                 self.scdType2(source=self.sourceTable, target=spark.read.load(self.TARGET_PATH), on=self.PrimaryKeyColumn, startDate= 'startDate', endDate='endDate', activeFlag='activeFlag')\
    #                     .write.mode('overwrite').save(self.TARGET_PATH)
    #         else:
    #             if self.tableInfo.loc['PartitionColumn']:
    #                 self.sourceTable.write.mode('overwrite').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).option('overwriteSchema','true').save(self.TARGET_PATH)
    #             else:
    #                 self.sourceTable.write.mode('overwrite').option('overwriteSchema','true').save(self.TARGET_PATH)

    #     elif self.TableType.lower() == 'transaction': # always append
    #         if self.tableInfo.loc['PartitionColumn']:
    #             self.sourceTable.write.mode('append').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).save(self.TARGET_PATH)
    #         else:
    #             self.sourceTable.write.mode('append').save(self.TARGET_PATH)

    #     else:
    #         raise NameError("wrong table type")
    
    def save(self):
        if self.TableType.lower() == 'transaction': # always append
            self.save_transaction(sourceTable=self.sourceTable, targetPath=self.TARGET_PATH, primarykey=self.PrimaryKeyColumn, partition=self.PartitionColumn)
        elif self.TableType.lower() == 'master':
            self.save_master(sourceTable=self.sourceTable, targetPath=self.TARGET_PATH, primarykey=self.PrimaryKeyColumn, partitionn=self.PartitionColumn, scd2flag=self.tableInfo.loc['SourceSCD2Flag'])
        else:
            raise NameError("wrong table type")

    def save_old(self):
        if self.TableType.lower() == 'master':
            if 1 - self.tableInfo.loc['SourceSCD2Flag']:
                # [x] TODO: Implement scd type 2 later -- 20250405
                if self.tableInfo.loc['PartitionColumn']:
                    self.scdType2(source=self.sourceTable, target=spark.read.load(self.TARGET_PATH), on=self.PrimaryKeyColumn, startDate= 'startDate', endDate='endDate', activeFlag='activeFlag')\
                        .write.mode('overwrite').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).save(self.TARGET_PATH)
                else:
                    self.scdType2(source=self.sourceTable, target=spark.read.load(self.TARGET_PATH), on=self.PrimaryKeyColumn, startDate= 'startDate', endDate='endDate', activeFlag='activeFlag')\
                        .write.mode('overwrite').save(self.TARGET_PATH)
            else:
                if self.tableInfo.loc['PartitionColumn']:
                    self.sourceTable.write.mode('overwrite').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).option('overwriteSchema','true').save(self.TARGET_PATH)
                else:
                    self.sourceTable.write.mode('overwrite').option('overwriteSchema','true').save(self.TARGET_PATH)

        elif self.TableType.lower() == 'transaction': # always append
            if self.tableInfo.loc['PartitionColumn']:
                self.sourceTable.write.mode('append').partitionBy(ast.literal_eval(self.tableInfo.loc['PartitionColumn'])).save(self.TARGET_PATH)
            else:
                self.sourceTable.write.mode('append').save(self.TARGET_PATH)

        else:
            raise NameError("wrong table type")

    def updateTableInfoAtLast(self):
        self.tableInfo.loc['LastRunTimestamp'] = self.current_ict_time

    def execute(self):
        if self.BrToSil_object.isInitial:
            for func in tqdm([self.loadSource, self.changeColumnName, self.applyType, self.DQ1, self.DQ2, self.DQ3, self.DQ4, self.DQ5, self.DQ6, self.DQ7, self.DQ8, self.save_initial, self.updateTableInfoAtLast], desc=f'{self.sourceTableName:.30s}',leave=True):
                func()
            return self.tableInfo # the updated of meta data such as "LastRunTimestamp" for incremental load condition
        else:
            for func in tqdm([self.loadSource, self.changeColumnName, self.applyType, self.DQ1, self.DQ2, self.DQ3, self.DQ4, self.DQ5, self.DQ6, self.DQ7, self.DQ8, self.save, self.updateTableInfoAtLast], desc=f'{self.sourceTableName:.30s}',leave=True):
                func()
            return self.tableInfo # the updated of meta data such as "LastRunTimestamp" for incremental load condition