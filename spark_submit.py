import os
import re
from pyspark.sql.functions import concat_ws,col,sha2
from pyspark.sql.types import DecimalType,StringType
from pyspark.sql import functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType
import json
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Transformation").getOrCreate()
spark.sparkContext.addPyFile("s3://rakeshv-landingzone-batch07/Delta_File/delta-core_2.12-0.8.0.jar")
from delta import *

data = ''.join(spark.sparkContext.textFile('s3://diprojit-landingzone-o7/sparkjob/app-config.json').collect())
jsonData = json.loads(data)
dataset = ["actives","viewership"]
#dataset = ["actives"]
conf = {}

for word in dataset:
  
  conf[word + "_source"] = jsonData['ingest-' + word]['source']['data-location']
  conf[word + "_destination"] = jsonData['ingest-'+ word]['destination']['data-location']
  conf['rawzone_'+ word + '_source'] = jsonData['masked-' + word]['source']['data-location']
  conf[word + '_mask_columns'] = jsonData['masked-' + word]['masking-cols']
  conf[word + '_transformation_columns'] = jsonData['masked-' + word]['transformation-cols']
  conf[word + '_partition_columns'] = jsonData['masked-' + word]['partition-cols']


class Transform():

    def read_data(self,path):
        df = spark.read.parquet(path)
        return df

    def write_data(self,df,path,format,partition_columns=[]):
        if partition_columns: 
            df.write.mode("append").partitionBy(partition_columns[0],partition_columns[1]).parquet(path)
        else:
            df.write.mode("append").parquet(path)
   
        
    #Writing actives data to staging zone
    def raw_to_staging(self,masked_data,word):
      
        destination_loc = jsonData['masked-'+ word]['destination']['data-location']
        self.write_data(masked_data,destination_loc,"parquet",conf[word + '_partition_columns']) 
  
    
    #Function to perform the operations of casting fields
    def transformation(self,df,cast_dict):
        items_list=[]
        for item in cast_dict.keys():
            items_list.append(item)

        for column in items_list:
            if cast_dict[column].split(",")[0] == "DecimalType":
                df = df.withColumn(column,df[column].cast(DecimalType(scale=int(cast_dict[column].split(",")[1]))))
            elif cast_dict[column] == "ArrayType-StringType":
                df = df.withColumn(column,f.concat_ws(",",'city','state','location_category'))
        return df
    
    
    #Funstion to perform the operation of masking specified fields
    def mask_data(self,df,column_list):
        for column in column_list:
            df = df.withColumn("masked_"+column,f.sha2(f.col(column),256))
        return df

    def SCD_implement(self,masked_DF):
        try:
            targetTable = DeltaTable.forPath(spark,"s3://diprojit-landingzone-o7/delta_table/")
            delta_DF = targetTable.toDF()
        except:
            emptyRDD = spark.sparkContext.emptyRDD()
            schema = StructType([
                StructField('advertising_id', StringType(), True),
                StructField('user_id', StringType(), True),
                StructField('masked_advertising_id', StringType(), True),
                StructField('masked_user_id', StringType(), True),
                StructField('start_date', StringType(), True),
                StructField('end_date', StringType(), True),
                StructField('status', StringType(), True),
                ])
            delta_DF = spark.createDataFrame(emptyRDD,schema)
            delta_DF.write.format("delta").mode("overwrite").save("s3://diprojit-landingzone-o7/delta_table/")

        masked_DF.createOrReplaceTempView("source")
        delta_DF.createOrReplaceTempView("target")
        
        #***********__Searching for unchanged rows and inactive rows__***********
        stage_1_DF = spark.sql("(select T.* from target T where T.status = 'inactive' ) union (select T.* from target T where T.status = 'active' and (T.advertising_id,T.user_id) in (select advertising_id,user_id from source))")

        #***********__searching for new rows__**************
        stage_2_DF = spark.sql("select S.*,'active' as status from source S where (S.advertising_id) not in (select advertising_id from target) ")
 

        #***********__searching for updated rows__************
        stage_3_DF = spark.sql("(select T.advertising_id,T.user_id,T.masked_advertising_id,T.masked_user_id,T.start_date,current_date() as end_date,'Inactive' as status from target T where (T.advertising_id) in (select advertising_id from source) and T.user_id not in (select user_id from source)) union (select S.*, 'Active' as status from source S where ((S.advertising_id) in (select advertising_id from target)) and ((S.user_id) not in (select user_id from target)))")
        

        DF_Final = stage_1_DF.union(stage_2_DF).union(stage_3_DF)
        DF_Final.write.format("delta").mode("overwrite").save("s3://diprojit-landingzone-o7/delta_table/")
        print(DF_Final.count())
        print(DF_Final.show(40))




t = Transform()
datasets = ['Actives','Viewership']

for word in datasets:
        try:
                #read dataset from landingzone
                #df_from_landing = t.read_data(conf[word+'_source'])

                #write dataset to rawzone
                #t.write_data(df_from_landing,conf[word+'_destination'],"parquet")
        
                #Reading data from rawzone
                df_from_raw = t.read_data(conf['rawzone_'+word.lower()+'_source'])
                
        
                #Casting the fields  dataset
                df_staging = t.transformation(df_from_raw, conf[word.lower() +'_transformation_columns'])
        
                #MAsking the fields of  dataset
                df_masking = t.mask_data(df_staging,conf[word.lower()+'_mask_columns'])
                if word == 'Actives':
                        t.SCD_implement(df_masking.select(col("advertising_id"),col("user_id"),col("masked_advertising_id"),col("masked_user_id")).withColumn("start_date",current_date().cast("string")).withColumn("end_date",lit("null")))

                    
                    
                #Writing dataframe to staging zone
                t.raw_to_staging(df_masking,word.lower())

        except Exception as e :
                print(e)
                print(word)

        
