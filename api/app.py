from flask import Flask, jsonify, request
from pipeline.ingestion import create_spark, load_data
from pipeline.transformation import transform_data
from pyspark.sql.functions import sum, col, count, to_date

app = Flask(__name__)

try:
    spark = create_spark()
    data = load_data(spark)
    df = transform_data(data)
except Exception as e:
    print("Startup Error:", e)
    df = None
    

# Ensure date column is usable
df = df.withColumn("appointment_date", to_date(col("appointment_date")))

@app.route("/revenue")
def revenue():
    start = request.args.get("start_date")
    end = request.args.get("end_date")

    temp = df
    if start and end:
        temp = temp.filter((col("appointment_date") >= start) & (col("appointment_date") <= end))

    total = temp.select(sum("amount")).collect()[0][0]
    if df is None:
        return jsonify({"error": "Data not available"})
    return jsonify({"total_revenue": round(float(total), 2)})


@app.route("/top-doctors")
def top_doctors():
    doctor = request.args.get("doctor")
    start = request.args.get("start_date")
    end = request.args.get("end_date")

    temp = df

    if doctor:
        temp = temp.filter(col("doctor_id") == doctor)

    if start and end:
        temp = temp.filter((col("appointment_date") >= start) & (col("appointment_date") <= end))

    result = temp.groupBy("doctor_id", "doctor_first_name") \
        .agg(sum("amount").alias("revenue")) \
        .orderBy(col("revenue").desc()) \
        .limit(5) \
        .toPandas()
    if df is None:
        return jsonify({"error": "Data not available"})

    return result.to_json(orient="records")


@app.route("/department-load")
def department_load():
    dept = request.args.get("dept")

    temp = df
    if dept:
        temp = temp.filter(col("specialization") == dept)

    result = temp.groupBy("specialization") \
        .agg(count("*").alias("count")) \
        .toPandas()
    if df is None:
        return jsonify({"error": "Data not available"})

    return result.to_json(orient="records")


@app.route("/trend")
def trend():
    result = df.groupBy("appointment_date") \
        .agg(sum("amount").alias("revenue")) \
        .orderBy("appointment_date") \
        .toPandas()
    if df is None:
        return jsonify({"error": "Data not available"})

    return result.to_json(orient="records")

print("🚀 Starting Flask API...")
app.run(host="0.0.0.0", port=5000)