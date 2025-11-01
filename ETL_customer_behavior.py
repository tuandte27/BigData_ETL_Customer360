from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer, util

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')

def most_search(data):
    data = data.groupBy("user_id", "keyword").count()
    data = data.orderBy(col("count").desc())
    data = data.withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(col("count").desc())))
    data = data.filter(col("rank") == 1).select("user_id", "keyword").withColumnRenamed("keyword", "most_search")
    return data

def generate_date(start_time, end_time):
    date_list = pd.date_range(start=start_time, end=end_time ).strftime('%Y%m%d').to_list()
    return date_list

def trending_type(df):
    df = df.withColumn("Trending_Type",
        when(col("Category_T6") == col("Category_T7"), "Unchanged")\
        .otherwise("Changed"))
    return df

def category_change(df):
    df = df.withColumn("Category_Change",
        when(col("Category_T6") != col("Category_T7"), concat_ws("-", col("Category_T6"), col("Category_T7"))
        ).otherwise("Unchanged")
    )
    return df

def join_with_simcse(df_spark, category_spark, col_df, col_cat, threshold=0.7):
    """
    Join gần đúng giữa hai dataframe Spark dựa trên độ similar (cosine similarity) 
    giữa các chuỗi được mã hóa bằng SimCSE.
    """
    # Convert sang pandas để tính embedding
    df = df_spark.select(col_df).distinct().toPandas()
    cat = category_spark.select(col_cat, "category").distinct().toPandas()

    # Encode text
    df_emb = model.encode(df[col_df].tolist(), convert_to_tensor=True)
    cat_emb = model.encode(cat[col_cat].tolist(), convert_to_tensor=True)

    # Tính cosine similarity
    cosine_scores = util.cos_sim(df_emb, cat_emb).cpu().numpy()

    # Tìm match tốt nhất cho mỗi từ khóa
    best_match = []
    for i, kw in enumerate(df[col_df]):
        j = np.argmax(cosine_scores[i])
        score = cosine_scores[i][j]
        if score >= threshold:
            best_match.append((kw, cat.iloc[j][col_cat], cat.iloc[j]["category"], float(score)))

    matched_df = pd.DataFrame(best_match, columns=[col_df, "matched_keyword", "category", "similarity"])
    
    # Chuyển ngược sang Spark và rename để tránh trùng cột
    matched_spark = spark.createDataFrame(matched_df)
    matched_spark = matched_spark.withColumnRenamed(col_df, f"{col_df}_key") \
                                 .withColumnRenamed("category", f"category_{col_df}")

    result = df_spark.join(
        matched_spark,
        df_spark[col_df] == matched_spark[f"{col_df}_key"],
        how="inner"
    ).drop(f"{col_df}_key")
    return result

def import_to_mysql(result):
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'customer360'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = ''
    result.write.format('jdbc') \
        .option('url', url) \
        .option('driver', driver) \
        .option('dbtable', 'summary_search_data') \
        .option('user', user) \
        .option('password', password) \
        .mode('overwrite') \
        .save()
    print("Data Import Successfully")

def main(path):
    start_date_T6 = "20220601"
    end_date_T6 = "20220614"
    start_date_T7 = "20220701"
    end_date_T7 = "20220714"
    date_list_T6 = generate_date(start_date_T6, end_date_T6)
    date_list_T7 = generate_date(start_date_T7, end_date_T7)

    # Load data T6
    print("------------Read data T6------------")
    df_T6 = spark.read.parquet(path + date_list_T6[0])
    for date in date_list_T6[1:]:
        df_T6 = df_T6.union(spark.read.parquet(path + date))
    df_T6.cache()
    df_T6 = most_search(df_T6)
    df_T6 = df_T6.withColumnRenamed("most_search", "most_search_T6")

    # Load data T7
    print("------------Read data T7------------")
    df_T7 = spark.read.parquet(path + date_list_T7[0])
    for date in date_list_T7[1:]:
        df_T7 = df_T7.union(spark.read.parquet(path + date))
    df_T7.cache()
    df_T7 = most_search(df_T7)
    df_T7 = df_T7.withColumnRenamed("most_search", "most_search_T7")

    # Đọc file category_search.csv
    category = spark.read.csv("data\\category_search.csv", header=True)

    # Join category với dữ liệu 2 tháng, độ chính xác trên 0.9
    df_T6 = join_with_simcse(df_T6, category, "most_search_T6", "most_search", threshold=0.9)
    df_T7 = join_with_simcse(df_T7, category, "most_search_T7", "most_search", threshold=0.9)

    df_T6 = df_T6.withColumnRenamed("category_most_search_T6", "category_T6")
    df_T7 = df_T7.withColumnRenamed("category_most_search_T7", "category_T7")

    # Join dữ liệu 2 tháng
    result = df_T6.join(df_T7, on="user_id", how="inner")
    result = result.select("user_id", "most_search_T6", "category_T6", "most_search_T7", "category_T7")
    result = trending_type(result)
    result = category_change(result)

    # Load vào MySQL
    import_to_mysql(result)

path = "data\\log_search\\"
main(path)
