from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IMDbAnalysis").getOrCreate()

# Đọc dữ liệu từ Hadoop (cập nhật đường dẫn đúng)
df_movies = spark.read.csv("hdfs://localhost:9000/path/to/title.basics.tsv", sep="\t", header=True, inferSchema=True)
df_ratings = spark.read.csv("hdfs://localhost:9000/path/to/title.ratings.tsv", sep="\t", header=True, inferSchema=True)

df_movies.printSchema()
df_ratings.printSchema()
print(f"Movies dataset: {df_movies.count()} records")
print(f"Ratings dataset: {df_ratings.count()} records")

#Làm sạch dữ liệu
from pyspark.sql.functions import col

# Loại bỏ các dòng có giá trị NULL
df_movies = df_movies.na.drop()
df_ratings = df_ratings.na.drop()

# Chỉ giữ lại các phim đã phát hành
df_movies = df_movies.filter(col("startYear").isNotNull())

# Đổi kiểu dữ liệu của cột startYear
df_movies = df_movies.withColumn("startYear", col("startYear").cast("int"))

df = df_movies.join(df_ratings, df_movies["tconst"] == df_ratings["tconst"], "inner") \
              .select(df_movies["tconst"], df_movies["primaryTitle"], df_movies["startYear"], df_ratings["averageRating"], df_ratings["numVotes"])

df.show(10)

#Phân tích xu hướng theo thờigian
from pyspark.sql.functions import avg

df_trend = df.groupBy("startYear").agg(avg("averageRating").alias("avg_rating"), avg("numVotes").alias("avg_votes"))
df_trend = df_trend.orderBy("startYear")
df_trend.show(10)

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Tạo feature vector
assembler = VectorAssembler(inputCols=["startYear"], outputCol="features")
df_train = assembler.transform(df_trend)

# Chia dữ liệu thành tập train và test
train_data, test_data = df_train.randomSplit([0.8, 0.2], seed=42)

#Huấn luyện
lr = LinearRegression(featuresCol="features", labelCol="avg_votes", maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_data)

# Dự đoán
predictions = lr_model.transform(test_data)
predictions.select("startYear", "avg_votes", "prediction").show(10)


#Trực quan
import matplotlib.pyplot as plt
import pandas as pd

# Chuyển đổi dữ liệu Spark thành Pandas
df_pd = predictions.select("startYear", "avg_votes", "prediction").toPandas()

plt.figure(figsize=(12, 6))
plt.plot(df_pd["startYear"], df_pd["avg_votes"], label="Actual Votes", color="blue")
plt.plot(df_pd["startYear"], df_pd["prediction"], label="Predicted Votes", color="red", linestyle="dashed")
plt.xlabel("Year")
plt.ylabel("Number of Votes")
plt.title("Trend of Movie Popularity Over Time")
plt.legend()
plt.show()
