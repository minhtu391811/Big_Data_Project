from Lambda.Batch_layer.text_to_doc import spark_tranform

def batch_layer():
    # Lấy DataFrame Spark đã biến đổi
    spark_df = spark_tranform()

    # Định nghĩa thông tin kết nối PostgreSQL
    jdbc_url = "jdbc:postgresql://@localhost:5432/my_database"
    properties = {
        "user": "postgres",
        "password": "Mop-391811",
        "driver": "org.postgresql.Driver"
    }

    # Lưu DataFrame Spark vào PostgreSQL
    spark_df.write \
        .jdbc(url=jdbc_url, table="<schema>.<table>", mode="append", properties=properties)
