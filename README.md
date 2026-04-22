# Healthcare-Data-Pipeline-using-PySpark-MedInsight-Simulation-
## 🚀 Overview
Built a production-style healthcare data pipeline using PySpark, implementing:
- Data ingestion from multi-table CSV datasets
- Schema enforcement
- Data validation (null & duplicate checks)
- Multi-table joins
- Business KPIs generation

---

## 🧱 Architecture

Raw Data → Ingestion → Validation → Transformation → KPIs

---

## ⚙️ Tech Stack

- PySpark
- Docker
- PostgreSQL (optional)
- Python

---

## 📊 KPIs Generated

- Total Revenue
- Top Doctors by Revenue
- Department Load
- Patient Visit Frequency

---

## 📁 Project Structure


pipeline/
ingestion.py
validation.py
transformation.py
kpi.py
api/
dashboard/
data/
docker/


---

## ▶️ How to Run

```bash
docker compose up --build -d
docker exec -it spark-container bash
cd /app
python -m pipeline.ingestion
💡 Key Highlights
Production-style ETL design (Bronze/Silver/Gold layers)
Schema-controlled ingestion (no inferSchema)
Data quality validation layer
Scalable PySpark transformations
📌 Future Improvements
Airflow orchestration
REST API (Flask)
Streamlit dashboard

---

# Step 6 — Push README Update

```bash
git add README.md
git commit -m "Added professional README with architecture and KPIs"
git push
'''