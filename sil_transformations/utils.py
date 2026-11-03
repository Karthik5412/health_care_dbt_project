from pyspark.sql import DataFrame, SparkSession 
from pyspark.sql.functions import concat,row_number,col,lit, current_timestamp, coalesce, lower
from pyspark.sql.window import Window 
from delta.tables import DeltaTable

class custom_trans :
    def __init__(self, df : DataFrame) :
        self.df = df

    def dedupe(self, cols : list) :
        
        self.df = self.df.withColumn('dedupekey', concat(*cols))
        self.df = self.df.withColumn('dedupecount', row_number().over(Window.partitionBy('dedupekey').orderBy(lit(1))))

        self.df = self.df.filter(col('dedupecount') == 1).drop('dedupekey','dedupecount')

        return self.df


    def processing_timestamp(self) :
        self.df = self.df.withColumn('processing_timestamp', current_timestamp() )

        return self.df
    

    def upsert(self, table : str , cols : list) : 
        statement = " AND ".join([f"src.{i} = trg.{i}" for i in cols]) 
        dlt_obj = DeltaTable.forName(SparkSession.getActiveSession(),f'health_care.silver.{table}')

        dlt_obj.alias('trg').merge(
            self.df.alias('src'),
            statement 
        ).whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

        return 




















