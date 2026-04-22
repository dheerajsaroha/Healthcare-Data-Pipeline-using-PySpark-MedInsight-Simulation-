from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pipeline.validation import validate_data
from pipeline.transformation import transform_data
from pipeline.kpi import compute_kpis

def create_spark():
    return SparkSession.builder \
        .appName("Healthcare Ingestion") \
        .getOrCreate()


def load_data(spark):

    base_path = "/app/data/raw/"

    # Patients Schema
    patients_schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("contact_number", StringType(), True),
    StructField("address", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("insurance_provider", StringType(), True),
    StructField("insurance_number", StringType(), True),
    StructField("email", StringType(), True)
])

    patients_df = spark.read.csv(
        base_path + "patients.csv",
        header=True,
        schema=patients_schema
    )

    # Doctors Schema
    doctors_schema = StructType([
    StructField("doctor_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("specialization", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("years_experience", IntegerType(), True),
    StructField("hospital_branch", StringType(), True),
    StructField("email", StringType(), True)
])

    doctors_df = spark.read.csv(
        base_path + "doctors.csv",
        header=True,
        schema=doctors_schema
    )

    # Appointments Schema
    appointments_schema = StructType([
    StructField("appointment_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("doctor_id", StringType(), True),
    StructField("appointment_date", StringType(), True),
    StructField("appointment_time", StringType(), True),
    StructField("reason_for_visit", StringType(), True),
    StructField("status", StringType(), True)
    ])

    appointments_df = spark.read.csv(
        base_path + "appointments.csv",
        header=True,
        schema=appointments_schema
    )

    # Treatments Schema
    treatments_schema = StructType([
    StructField("treatment_id", StringType(), True),
    StructField("appointment_id", StringType(), True),
    StructField("treatment_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("cost", DoubleType(), True),
    StructField("treatment_date", StringType(), True)
    ])

    treatments_df = spark.read.csv(
        base_path + "treatments.csv",
        header=True,
        schema=treatments_schema
    )

    # Billing Schema
    billing_schema = StructType([
    StructField("bill_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("treatment_id", StringType(), True),
    StructField("bill_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True)
    ])

    billing_df = spark.read.csv(
        base_path + "billing.csv",
        header=True,
        schema=billing_schema
    )

    return {
        "patients": patients_df,
        "doctors": doctors_df,
        "appointments": appointments_df,
        "treatments": treatments_df,
        "billing": billing_df
    }






if __name__ == "__main__":
    spark = create_spark()
    data = load_data(spark)

    for name, df in data.items():
        print(f"\n=== {name.upper()} ===")
        df.printSchema()
        df.show(5)
    validate_data(data)
    transformed_df = transform_data(data)
    print("\n=== TRANSFORMED DATA ===")
    # print(transformed_df.show(5))
    transformed_df.toPandas().to_csv("data/processed/final_data.csv", index=False)
    compute_kpis(transformed_df)

