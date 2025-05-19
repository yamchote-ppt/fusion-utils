# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType, DecimalType, StringType, LongType, TimestampType, StructType, StructField, DoubleType, FloatType, ShortType,BooleanType,DateType,FloatType
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from typing import Union, List, Tuple, Any
import notebookutils

# from tabulate import tabulate

spark = SparkSession.builder\
        .appName("utils")\
        .getOrCreate()

class AuditLog_Fusion:

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
    
    def __init__(self, columns: Union[List[str], Tuple[str, ...]], WS_ID: str, TABLE_NAME_to_check:str, AUDIT_TABLE_NAME:str, LH_ID_to_check: str, LH_ID_audit: str = None, schema_check: str = None, schema_audit: str = None):
        '''
        - if `LH_ID_audit` is not given, it is  LH_ID_to_check automatically, i.e. audit table is in the same lakehouse as that of
        - if using lakehouse with Schema, please provide `schema` parameter
        '''
        self.WS_ID = WS_ID
        self.TABLE_NAME_to_check = TABLE_NAME_to_check
        self.AUDIT_TABLE_NAME = AUDIT_TABLE_NAME
        self.LH_ID_to_check = LH_ID_to_check
        self.LH_ID_audit = LH_ID_audit if LH_ID_audit else LH_ID_to_check
        self.schema_check = schema_check
        self.schema_audit = schema_audit
        self.fixColumns = {'STARTTIME','ENDTIME','AUDITKEY','STATUS_ACTIVITY'}
        self.columns = tuple(set(columns).union(self.fixColumns))
        
        if self.schema_check:
            self.PATH_TO_CHECKED_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_to_check}/Tables/{self.schema_check}/{self.TABLE_NAME_to_check}'
        else:
            self.PATH_TO_CHECKED_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_to_check}/Tables/{self.TABLE_NAME_to_check}'


        if self.schema_audit:    
            self.PATH_TO_AUDIT_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_audit}/Tables/{self.schema_audit}/{self.AUDIT_TABLE_NAME}'
        else:
            self.PATH_TO_AUDIT_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_audit}/Tables/{self.AUDIT_TABLE_NAME}'
    
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
        # df.write.mode('overwrite').option('overwriteSchema', 'true').save(self.PATH_TO_AUDIT_TABLE)
        return df

    def getAuditLogTable(self):
        return spark.read.load(self.PATH_TO_AUDIT_TABLE)
    
    def countBefore(self):
        if self.log['COUNTROWSBEFORE']:
            raise ValueError('COUNTROWSBEFORE already exist')
        if notebookutils.fs.exists(self.PATH_TO_CHECKED_TABLE):
            self.log['COUNTROWSBEFORE'] = spark.read.load(self.PATH_TO_CHECKED_TABLE).count()
        else:
            self.log['COUNTROWSBEFORE'] = 0

    def countAfter(self):
        self.log['COUNTROWSAFTER'] = spark.read.load(self.PATH_TO_CHECKED_TABLE).count()

    def getAllPath(self):
        return {'PATH_TO_AUDIT_TABLE':self.PATH_TO_AUDIT_TABLE, 'PATH_TO_CHECKED_TABLE':self.PATH_TO_CHECKED_TABLE}

    def startAudit(self):
        self.log['STARTTIME'] = str(datetime.now() + timedelta(hours=7))
        self.log['STATUS_ACTIVITY'] = 'logging ...'

class AuditLog_STGT(AuditLog_Fusion):
    
    def __init__(self, WS_ID: str, TABLE_NAME_to_check:str, AUDIT_TABLE_NAME:str, LH_ID_to_check: str, LH_ID_audit: str = None, schema_check: str = None, schema_audit: str = None):
        '''
        - if `LH_ID_audit` is not given, it is  LH_ID_to_check automatically, i.e. audit table is in the same lakehouse as that of
        - if using lakehouse with Schema, please provide `schema` parameter
        '''
        super().__init__(['PIPELINENAME', 'PIPELINERUNID', 'TRIGGERTYPE', 'TABLE_NAME', 'FUNCTION_NAME','COUNTROWSBEFORE', 'COUNTROWSAFTER', 'ERRORCODE', 'ERRORMESSAGE'] ,WS_ID, TABLE_NAME_to_check, AUDIT_TABLE_NAME, LH_ID_to_check, LH_ID_audit, schema_check, schema_audit)

    def initialDetail(self,  pipelineName: str, pipelineId: str, TriggerType: str, TableName: str, functionName: str):
        super().initialDetail({
            'PIPELINENAME': pipelineName, 
            'PIPELINERUNID': pipelineId, 
            'TRIGGERTYPE': TriggerType, 
            'TABLE_NAME': TableName, 
            'FUNCTION_NAME': functionName
        })
        self.startAudit()
        self.log['AUDITKEY'] = self.log['PIPELINENAME'] + '-' + self.log['TABLE_NAME'] + '-' + str(self.log['STARTTIME']).replace(' ','_').replace(':','_')

    def execute(self, ETL_func, raiseError = True):
        try:
            self.countBefore()
            result = ETL_func()
            self.countAfter()
            self.endSuccess()
            return result
        except Exception as e:
            self.endFail(errorCode = '-', errorMessage = e)
            if not raiseError:
                raise e

def trim_string_columns(df):
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

def fillNaAll(df):
    fillnaDefault = {
        StringType: '',
        ShortType: 0,
        IntegerType: 0,
        DoubleType: 0.0,
        TimestampType: '1970-01-01 00:00:00',
        LongType:0,
        BooleanType: True,
        DecimalType:0.0,
        DateType: '1970-01-01',
        FloatType: 0.0,
    }
    fill_values = {}
    for field in df.schema.fields:
        field_type = type(field.dataType)
        if field_type in fillnaDefault:
            fill_values[field.name] = fillnaDefault[field_type]
        else:
            raise TypeError(f'No fill value defined for type {field_type}')
    return df.fillna(fill_values)

def fillNaAllStringType(df):
    fillnaDefault = {
        StringType: 'NA',
    }
    fill_values = {}
    for field in df.schema.fields:
        field_type = type(field.dataType)
        if field_type in fillnaDefault:
            fill_values[field.name] = fillnaDefault[field_type]
    return df.fillna(fill_values)

def copySchemaByName(df,fromDf):
    '''
    schemaField can be get by df.schema.fields
    '''
    dfCol = df.columns
    for column in fromDf.schema.fields:
        if column.name in dfCol: # [] TODO: don't forget to add to other Notebook
            df = df.withColumn(column.name,col(column.name).cast(column.dataType))
    return df

def getSetColumn(df,columnName):
    return set(df.select(columnName).toPandas()[columnName])

def getCountColumn(df,columnName):
    return df.select(columnName).toPandas()[columnName].value_counts()

def trackSizeTable(df,detail=None,schema = False,table=False):
    if detail:
        print(detail,end=': size = ')
    
    numrow = df.count()
    print(f'({numrow}, {len(df.columns)})')
    
    if schema:
        df.printSchema()
    
    if table:
        df.show()

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

def get_environment():
    id_detail = notebookutils.lakehouse.get("STGT_BRONZE_LH")
    work_id = id_detail.get('workspaceId')
    br_id = id_detail.get('id')

    sil_id_detail = notebookutils.lakehouse.get("STGT_SILVER_LH")
    sil_id = sil_id_detail.get('id')

    # Determine environment and key_v based on work_id prefix
    if str(work_id).startswith('9030'):
        env = 'Dev'
        key_v = 'kv-sta-dp-nonprodv2'
    elif str(work_id).startswith('bb99'):
        env = 'UAT'
        key_v = 'kv-sta-dp-uat'
    elif str(work_id).startswith('c225'):
        env = 'Production'
        key_v = 'kv-sta-dp-prod-it'
    else:
        env = 'Unknown'  # Fallback case
        key_v = 'unknown'

    return {
        "workspace_id": work_id,
        "br_id": br_id,
        "sil_id": sil_id,
        "env": env,
        "key_v": key_v
    }