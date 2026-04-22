from pyspark.sql.functions import col

def transform_data(data):

    p = data["patients"].alias("p")
    d = data["doctors"].alias("d")
    a = data["appointments"].alias("a")
    t = data["treatments"].alias("t")
    b = data["billing"].alias("b")

    df = a.join(p, col("a.patient_id") == col("p.patient_id")) \
          .join(d, col("a.doctor_id") == col("d.doctor_id")) \
          .join(t, col("a.appointment_id") == col("t.appointment_id")) \
          .join(b, (col("p.patient_id") == col("b.patient_id")) & 
                   (col("t.treatment_id") == col("b.treatment_id")))

    df = df.select(
        col("p.patient_id"),
        col("p.first_name").alias("patient_first_name"),
        col("p.last_name").alias("patient_last_name"),
        col("p.gender"),
        col("d.doctor_id"),
        col("d.first_name").alias("doctor_first_name"),
        col("d.last_name").alias("doctor_last_name"),
        col("d.specialization"),
        col("a.appointment_id"),
        col("a.appointment_date"),
        col("a.status"),
        col("t.treatment_type"),
        col("t.cost"),
        col("b.amount"),
        col("b.payment_status")
    )

    return df