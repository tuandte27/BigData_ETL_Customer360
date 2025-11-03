from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd
import json
import google.generativeai as genai

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

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

def generate_category(df, col_name, batch_size=300): 
    GOOGLE_API_KEY = "AIzaSyBe8OW5oMIBniw5s36h_WCA-R8n4EajSWg"
    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash')
    print("Đã kết nối với Google AI Studio (Gemini 2.5 Flash)")

    df = df.limit(10000).toPandas()
    
    if col_name not in df.columns:
        raise ValueError(f"Cột '{col_name}' không tồn tại trong dataframe")
    
    df.rename(columns={col_name: 'keyword'}, inplace=True)
    
    unique_keywords = df['keyword'].dropna().astype(str).unique().tolist()
    
    if not unique_keywords:
        df['category'] = "Other"
        return df

    all_mappings = {}
    total_batches = (len(unique_keywords) + batch_size - 1) // batch_size
    
    for i in range(0, len(unique_keywords), batch_size):
        current_batch = unique_keywords[i:i + batch_size]
        batch_index = i // batch_size + 1
        print(f"Đang xử lý Batch {batch_index}/{total_batches} ({len(current_batch)} keywords)")

        prompt = f"""
        Bạn là một chuyên gia phân loại nội dung phim, chương trình truyền hình và các loại nội dung giải trí. 
        Bạn sẽ nhận một danh sách tên có thể viết sai, viết liền không dấu, viết tắt, hoặc chỉ là cụm từ liên quan
        đến nội dung.

        ⚠️ Nguyên tắc quan trọng:
        - Không được trả về "Other" nếu có thể đoán được dù chỉ một phần ý nghĩa. 
        - Luôn cố gắng sửa lỗi, nhận diện tên gần đúng hoặc đoán thể loại gần đúng. 
        - Nếu không chắc → chọn thể loại gần nhất (VD: từ mô tả tình cảm → Romance, tên địa danh thể thao → Sports, 
        chương trình giải trí → Reality Show, v.v.)

        Nhiệm vụ của bạn:
        1. **Chuẩn hoá tên**: thêm dấu tiếng Việt nếu cần, tách từ, chỉnh chính tả.
        2. **Nhận diện tên hoặc ý nghĩa gốc gần đúng nhất**. Bao gồm:
        - Tên phim, series, show, chương trình
        - Quốc gia / đội tuyển (→ "Sports" hoặc "News")
        - Từ khoá mô tả nội dung
        3. **Gán thể loại phù hợp nhất** trong các nhóm sau: 
        - Action 
        - Romance 
        - Comedy 
        - Horror 
        - Animation 
        - Drama 
        - C Drama 
        - K Drama 
        - Sports 
        - Music 
        - Reality Show 
        - TV Channel 
        - News 
        - Other

        Một số quy tắc gợi ý nhanh:
        - Có từ “VTV”, “HTV”, “Channel” → TV Channel 
        - Có “running”, “master key”, “reality” → Reality Show 
        - Quốc gia, CLB bóng đá, sự kiện thể thao → Sports hoặc News 
        - “sex”, “romantic”, “love” → Romance 
        - “potter”, “hogwarts” → Drama / Fantasy 
        - Tên phim Việt/Trung/Hàn → ưu tiên Drama / C Drama / K Drama

        Chỉ trả về **1 JSON object**. 
        Key = tên gốc trong danh sách. 
        Value = thể loại đã phân loại.

        Ví dụ: 
        {{
        "thuyếtminh": "Other",
        "bigfoot": "Horror",
        "capdoi": "Romance",
        "ARGEN": "Sports",
        "nhật ký": "Drama",
        "PENT": "C Drama",
        "running": "Reality Show",
        "VTV3": "TV Channel"
        }}

        Danh sách:
        {current_batch}
        """
        
        try:
            resp = model.generate_content(prompt)
            text = resp.text.strip()
            start, end = text.find("{"), text.rfind("}")
            
            if start == -1 or end == -1:
                mapping = {m: "Other" for m in current_batch}
            else:
                json_text = text[start:end+1]
                parsed = json.loads(json_text)
                mapping = {title: parsed.get(title, "Other") for title in current_batch} 
            
            all_mappings.update(mapping)
            
        except Exception as e:
            print(f"LỖI API ở Batch {i // batch_size + 1}: {e}")
            error_mapping = {m: "Other" for m in current_batch}
            all_mappings.update(error_mapping)
    
    df['category'] = df['keyword'].map(lambda x: all_mappings.get(x, "Other"))
    
    return df

def category_change(df):
    df = df.withColumn("Category_Change",
        when(col("Category_T6") != col("Category_T7"), concat_ws("-", col("Category_T6"), col("Category_T7"))
        ).otherwise("Unchanged")
    )
    return df

def import_to_mysql(result):
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'customer360'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = ''
    result.write.format('jdbc') \
        .option('url', url) \
        .option('driver', driver) \
        .option('dbtable', 'behavior_data') \
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

    # Sinh category cho 2 tháng
    df_T6 = generate_category(df_T6, col_name="most_search_T6")
    df_T7 = generate_category(df_T7, col_name="most_search_T7")

    df_T6 = spark.createDataFrame(df_T6).withColumnRenamed("keyword", "most_search_T6")\
            .withColumnRenamed("category", "category_T6")
    df_T7 = spark.createDataFrame(df_T7).withColumnRenamed("keyword", "most_search_T7")\
            .withColumnRenamed("category", "category_T7")

    # Join dữ liệu 2 tháng
    result = df_T6.join(df_T7, on="user_id", how="inner")
    result = result.select("user_id", "most_search_T6", "category_T6", "most_search_T7", "category_T7")
    print("------------Trending Type------------")
    result = trending_type(result)
    print("------------Category Change------------")
    result = category_change(result)
    result.show()
    # Load vào MySQL
    import_to_mysql(result)

path = "data\\log_search\\"
main(path)