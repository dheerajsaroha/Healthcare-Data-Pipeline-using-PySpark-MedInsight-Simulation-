from pyspark.sql.functions import sum, count, col


def compute_kpis(df):

    print("\n💰 Total Revenue")
    df.select(sum("amount").alias("total_revenue")).show()

    print("\n👨‍⚕️ Top Doctors by Revenue")
    df.groupBy("doctor_id", "doctor_first_name") \
      .agg(sum("amount").alias("revenue")) \
      .orderBy(col("revenue").desc()) \
      .show(5)

    print("\n🏥 Department Load (Specialization)")
    df.groupBy("specialization") \
      .agg(count("*").alias("total_cases")) \
      .orderBy(col("total_cases").desc()) \
      .show()

    print("\n👥 Patient Visit Frequency")
    df.groupBy("patient_id") \
      .agg(count("*").alias("visit_count")) \
      .orderBy(col("visit_count").desc()) \
      .show(5)