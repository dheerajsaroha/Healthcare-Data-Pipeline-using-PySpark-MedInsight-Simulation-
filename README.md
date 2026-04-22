# Healthcare-Data-Pipeline-using-PySpark-MedInsight-Simulation-
## 🚀 Overview
This project demonstrates a **production-style data engineering pipeline** built using PySpark, Docker, Flask, and Streamlit.
It simulates a real-world healthcare analytics system by processing multi-source datasets and exposing insights through APIs and an interactive dashboard.
Built a production-style healthcare data pipeline using PySpark, implementing:
- Data ingestion from multi-table CSV datasets
- Schema enforcement
- Data validation (null & duplicate checks)
- Multi-table joins
- Business KPIs generation

---

## 🧱 Architecture

Raw Data → PySpark ETL → Data Validation → Transformation → KPIs Layer → Flask API → Streamlit → Dashboard

---

## ⚙️ Tech Stack

- **Data Processing:** PySpark  
- **Backend API:** Flask  
- **Frontend Dashboard:** Streamlit  
- **Database:** PostgreSQL  
- **Containerization:** Docker  

---
## 📊 Key Features

- ✅ Distributed data processing using PySpark  
- ✅ Schema enforcement & data validation (null & duplicate checks)  
- ✅ Multi-table joins across healthcare datasets  
- ✅ 📊 KPIs Generated:
  - Total Revenue
  - Top Doctors
  - Department Load
  - Patient Visit Frequency  
- ✅ REST API with filter support:
  - Date range filtering
  - Doctor filtering
  - Department filtering  
- ✅ Interactive dashboard with real-time insights 

---

## 📸 Screenshots

### Dashboard Overview
![Dashboard](docs/dashboard_overview.png)

### Filtered Dashboard
![Filtered](docs/dashboard_filtered.png)

### API Response
![API](docs/api_response.png)

### Docker Containers
![Docker](docs/docker_containers.png)

---

## ▶️ How to Run

```bash
# Start services
docker compose up --build -d

# Access dashboard
http://localhost:8501

# Access API
http://localhost:5000/revenue

```

## 📁 Project Structure

pipeline/
  ingestion.py
  validation.py
  transformation.py
  kpi.py

api/
  app.py

dashboard/
  app.py

data/
  raw/
  processed/

docker/
  Dockerfile

docker-compose.yml

---


# Step 6 — Push README Update

```bash
git add README.md
git commit -m "Added professional README with architecture and KPIs"
git push
```
# 💡 Key Highlights
Built using layered architecture (Bronze → Silver → Gold)
Designed API-first approach for scalability
Implements real-world data engineering patterns
Fully containerized for reproducibility

# 🚀 Future Enhancements
Airflow orchestration
CI/CD pipeline
Cloud deployment (AWS / GCP)
Advanced analytics & ML integration

👨‍💻 Author

Dheeraj Saroha
B.Tech AI & ML | Data Engineering Enthusiast

---
