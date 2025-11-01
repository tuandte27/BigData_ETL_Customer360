from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

#Phân loại AppName thành các Category
def transform_category(df):
	df = df.withColumn("Type",
		   when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyen Hinh")
		  .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
				 (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyen")
		  .when((col("AppName") == 'RELAX'), "Giai Tri")
		  .when((col("AppName") == 'CHILD'), "Thieu Nhi")
		  .when((col("AppName") == 'SPORT'), "The Thao")
		  .otherwise("Error"))
	df = df.select('Contract','Type','TotalDuration')
	df = df.filter(df.Contract != '0' )
	df = df.filter(df.Type != 'Error')
	return df

def most_watched(df):
	type_cols = ['Total_Truyen_Hinh', 'Total_Phim_Truyen', 'Total_Giai_Tri', 'Total_Thieu_Nhi', 'Total_The_Thao']
	df = df.withColumn('max', greatest(*type_cols)).withColumn('Most Watched',
		   when(col('max') == col('Total_Truyen_Hinh'), 'Truyen Hinh')
		  .when(col('max') == col('Total_Phim_Truyen'), 'Phim Truyen')
		  .when(col('max') == col('Total_Giai_Tri'), 'Giai Tri')
		  .when(col('max') == col('Total_Thieu_Nhi'), 'Thieu Nhi')
		  .when(col('max') == col('Total_The_Thao'), 'The Thao')
		  .otherwise('Error')).drop('max')
	return df

def customer_taste(df):
    df = df.withColumn("Taste",concat_ws("-",
                                    when(col("Total_Giai_Tri") != 0,lit("Giai Tri"))
                                    ,when(col("Total_Phim_Truyen")!= 0,lit("Phim Truyen"))
                                    ,when(col("Total_The_Thao")!= 0,lit("The Thao"))
                                    ,when(col("Total_Thieu_Nhi")!= 0,lit("Thieu Nhi"))
                                    ,when(col("Total_Truyen_Hinh")!= 0,lit("Truyen Hinh"))))
    return df

def agg_and_find_active(df):
	df = df.groupBy("Contract").agg(
		sum("Giai Tri").alias("Total_Giai_Tri"),
		sum("Phim Truyen").alias("Total_Phim_Truyen"),
		sum("The Thao").alias("Total_The_Thao"),
		sum("Thieu Nhi").alias("Total_Thieu_Nhi"),
		sum("Truyen Hinh").alias("Total_Truyen_Hinh"),
		countDistinct("Date").alias("Active")
    )
	df = most_watched(df)
	df = customer_taste(df)
	df = df.withColumn("Level_Activeness",
                       when(col("Active") > 20, "High")\
					   .when((col("Active") <= 20) & (col("Active") >= 10), "Medium")\
					   .otherwise("Low"))

	return df
	

def etl_1_day(path, path_day):
	print('-------------Reading data from path--------------')
	df = spark.read.json(path + path_day + ".json")
	print('-------------Transforming Category --------------')
	df = df.select("_source.*")
	df = transform_category(df)
	print('-------------Pivoting Data --------------')
	df = df.groupBy("Contract").pivot("Type").sum("TotalDuration").fillna(0)
	df = df.withColumn("Date", to_date(lit(path_day), "yyyyMMdd"))
	return df

def import_to_mysql(result):
	url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'customer360'
	driver = "com.mysql.cj.jdbc.Driver"
	user = 'root'
	password = ''
	result.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','interaction_data')\
	.option('user',user).option('password',password).mode('overwrite').save()
	return print("Data Import Successfully")

def main(path):
	start_date="20220401"
	end_date="20220430"
	date_list = pd.date_range(start= start_date ,end = end_date).strftime('%Y%m%d').tolist()
	print("ETL data file " + date_list[0] +".json")
	result = etl_1_day(path, date_list[0])
	
	for date in date_list[1:]:
		print("ETL data file " + date +".json")
		df_day = etl_1_day(path, date)
		result = result.unionByName(df_day)
	result = result.cache()
	result = result.fillna(0)
	#Tổng hợp và tính chỉ số hoạt động
	result = agg_and_find_active(result)
	#Load vào MySQL
	import_to_mysql(result)

path = "data\\log_content_sample\\"
main(path)