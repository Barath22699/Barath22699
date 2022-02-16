# Importing necessary packages

import json
from pyspark.sql.types import DecimalType,StringType
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
import sys
import boto3
from pyspark.sql.utils import AnalysisException

# Initializing Spark

spark = SparkSession.builder.appName("Demo").getOrCreate()

# For Delta

spark.sparkContext.addPyFile("/usr/lib/spark/jars/delta-core_2.12-0.8.0.jar")

from delta import DeltaTable


class Configuration:
    def __init__(self,spark_config_loc,datasetName, dataset_path):

        self.s3 = boto3.resource('s3')

        #set spark configuration
        self.setSparkConfig(spark_config_loc)

        # Extracting information from app_config file
        self.jsonData = self.read_config()

        # Ingesting data location to raw zone from landing zone
        
        #self.ingest_source = self.jsonData['ingest-dataset']['source']['data-location']
        #self.ingest_dest = self.jsonData['ingest-dataset']['destination']['data-location']
        
        # Ingesting file format to raw zone from landing zone
        
        #self.ingest_source_format = self.jsonData['ingest-dataset']['source']['file-format']
        #self.ingest_dest_format = self.jsonData['ingest-dataset']['destination']['file-format']
        
        # Reading data location to be read from raw zone
        
        self.mask_source = self.jsonData['masked-dataset']['source']['data-location'] + dataset_path
        self.mask_dest = self.jsonData['masked-dataset']['destination']['data-location'] + ("/".join(dataset_path.split("/")[:len(dataset_path.split("/"))-1]))
        
        # Reading file format to be read from raw zone
        
        self.mask_source_format = self.jsonData['masked-dataset']['source']['file-format']
        self.mask_dest_format = self.jsonData['masked-dataset']['destination']['file-format']

        # Reading masking-columns required to be done
        
        self.mask_cols = self.jsonData['masked-dataset']['masking-cols']
        
        # Reading lookup-columns and columns required to be done
        
        self.lookup_location = self.jsonData['lookup-dataset']['data-location']
        self.pii_cols = self.jsonData['lookup-dataset']['pii-cols']
        
        # Reading transformations-columns required to be done
        self.transformation_cols = self.jsonData['masked-dataset']['transformation-cols'] 

        # Reading partition-columns required to be done
        self.partition_cols = self.jsonData['masked-dataset']['partition-cols'] 
    
    # Sets spark configs from location fetched from livy call
    def setSparkConfig(self,location):
        location_list = location.replace(":","").split("/")
        
        obj = self.s3.Object(location_list[2], "/".join(location_list[3:]))
        body = obj.get()['Body'].read()
        json_raw = json.loads(body)
        spark_properties = json_raw['Properties']
        lst = list()
        
        for i in spark_properties:
            lst.append((i,spark_properties[i]))
            
        conf = spark.sparkContext._conf.setAll(lst)
        
    # fetchConfig is used to get app_config file from s3 bucket   
    def read_config(self):
        path = spark.sparkContext._conf.get('spark.path')
        path_list = path.replace(":","").split("/")
        obj = self.s3.Object(path_list[2], "/".join(path_list[3:]))
        body = obj.get()['Body'].read()
        jsonData = json.loads(body)
        
        return jsonData

class Transformation:
    def reading_data(self, path,format):
        try:
            if format == "parquet":
                df=spark.read.parquet(path)
                return df
            elif format == "csv":
                print(self.path)
                df=spark.read.csv(path + "." + format)
                print(df)
                return df
        except Exception as e:
            return e
        
    def write_data(self, df,path,format):
        try:
            if format == "parquet":
                df.write.mode("append").parquet(path)
            elif format == "csv":
                df.write.csv(path + "." + format)
            return "successfully written"
        except Exception as e:
            return e
        
    def partitioned_write_data(self, df,partition_cols,path,format):
        try:
            if format == "parquet":
                df.write.partitionBy(partition_cols).mode("append").parquet(path)
                return "successfully written"
        except Exception as e:
            return e 
            
    def masking(self,df,col_name):
        for columns in col_name:
            if columns in df.columns:
                df = df.withColumn("masked_"+columns,f.sha2(f.col(columns),256))
        return df
        
    def casting(self, df, cast_dict):
        key_list = []
        for key in cast_dict.keys():
            key_list.append(key)
        
        for column in key_list:
            if cast_dict[column].split(",")[0] == "DecimalType":
                df = df.withColumn(column,df[column].cast(DecimalType(scale=int(cast_dict[column].split(",")[1]))))
            elif cast_dict[column] == "ArrayType-StringType":
                df = df.withColumn(column,f.concat_ws(",",f.col(column)))
            elif cast_dict[column] == "StringType":
                df = df.withColumn(column,df[column].cast(StringType))
        
        return df
        
class Scd2:
    
    def lookup_dataset(self,df,lookup_location,pii_cols,datasetName):
    
        source_df = df.withColumn("begin_date",f.current_date())
        source_df = source_df.withColumn("update_date",f.lit("null"))
        
        pii_cols = [i for i in pii_cols if i in df.columns]
            
        columns_needed = []
        insert_dict = {}
        for col in pii_cols:
            if col in df.columns:
                columns_needed += [col,"masked_"+col]
        columns_needed_source = columns_needed + ['begin_date','update_date']

        source_df = source_df.select(*columns_needed_source)

        try:
            targetTable = DeltaTable.forPath(spark,lookup_location+datasetName)
        except AnalysisException:
            print('Table not found')
            source_df = source_df.withColumn("active_flag",f.lit("true"))
            source_df.write.format("delta").save(lookup_location+datasetName)
            print('Table Created')
            targetTable = DeltaTable.forPath(spark,lookup_location+datasetName)

        for i in columns_needed:
            insert_dict[i] = "updates."+i
            
        insert_dict['begin_date'] = f.current_date()
        insert_dict['active_flag'] = "true" 
        insert_dict['update_date'] = "null"

        _condition = datasetName+".active_flag == true AND "+" OR ".join(["updates."+i+" <> "+ datasetName+"."+i for i in [x for x in columns_needed if x.startswith("masked_")]])

        column = ",".join([datasetName+"."+i for i in [x for x in pii_cols]])

        updatedColumnsToInsert = source_df.alias("updates").join(targetTable.toDF().alias(datasetName), pii_cols).where(_condition)

        stagedUpdates = (
          updatedColumnsToInsert.selectExpr('NULL as mergeKey',*[f"updates.{i}" for i in source_df.columns]).union(source_df.selectExpr("concat("+','.join([x for x in pii_cols])+") as mergeKey", "*")))

        targetTable.alias(datasetName).merge(stagedUpdates.alias("updates"),"concat("+str(column)+") = mergeKey").whenMatchedUpdate(
            condition = _condition,
            set = {                  # Set current to false and endDate to source's effective date."active_flag" : "False",
            "active_flag" : "false",
            "update_date" : f.current_date()
          }
        ).whenNotMatchedInsert(
          values = insert_dict
        ).execute()

        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("masked_"+i, i)

        return df
        
        
if __name__=='__main__':
    
    spark_config_loc = sys.argv[1]
    
    datasetName = sys.argv[2]
    
    dataset_path = sys.argv[3]
        
    # Creating object
    
    obj = Transformation()

    scd2_obj = Scd2()
    
    conf_obj = Configuration(spark_config_loc,datasetName, dataset_path)
        
    # Reading the data from Landing zone

    #df = obj.reading_data(conf_obj.ingest_source,conf_obj.ingest_source_format)

    # Writing the same data directly to Raw zone

    #obj.write_data(df,conf_obj.ingest_dest,conf_obj.ingest_dest_format)

    # Reading the data again the Raw zone

    df = obj.reading_data(conf_obj.mask_source,conf_obj.mask_source_format)
        
    # Casting the required columns

    df = obj.casting(df,conf_obj.transformation_cols)

    # Masking the required columns

    df = obj.masking(df, conf_obj.mask_cols)
    
    # Creating a lookup dataset for the masking columns
    
    df = scd2_obj.lookup_dataset(df,conf_obj.lookup_location,conf_obj.pii_cols,datasetName)
    
    # Writing the data to Staging zone after transformation

    obj.write_data(df,conf_obj.mask_dest,conf_obj.mask_dest_format)

    # Partitioning the required columns and writing the data to Staging zone  

    #obj.partitioned_write_data(df, conf_obj.partition_cols, conf_obj.mask_dest, conf_obj.mask_dest_format)
      
    # stop the spark session

    spark.stop()
    
