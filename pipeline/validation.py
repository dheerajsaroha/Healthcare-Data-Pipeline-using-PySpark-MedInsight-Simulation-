from pyspark.sql.functions import col, count


def check_nulls(df, name):
    print(f"\n🔍 Null Check: {name}")
    df.select([
        count(col(c)).alias(c) for c in df.columns
    ]).show()


def check_duplicates(df, key, name):
    print(f"\n🔁 Duplicate Check: {name}")
    df.groupBy(key).count().filter("count > 1").show()


def validate_data(data):
    # Null checks
    for name, df in data.items():
        check_nulls(df, name)

    # Duplicate checks
    check_duplicates(data["patients"], "patient_id", "patients")
    check_duplicates(data["doctors"], "doctor_id", "doctors")
    check_duplicates(data["appointments"], "appointment_id", "appointments")
    check_duplicates(data["treatments"], "treatment_id", "treatments")
    check_duplicates(data["billing"], "bill_id", "billing")