# Data Engineering Project - Terrific Totes

## 🤝 Meet The Team
Funny Silly Crew In Full Action 
- Fidele
- Sukanya
- Charlotte
- Iohane
- Freddie
- Alyona


## 🔰 Overview
We have been approached by Terrific Totes to create a data-pipeline to Extract, Transform, and Load data from a prepared source into a data lake and warehouse hosted in AWS.

We will deliver a data platform that extracts data from an operational OLTP database, archives it in a data lake, and makes it available in a remodelled OLAP data warehouse.

## 🔧 Tech Stack
- Github - Repository management, CI/CD (Github-Actions), Credentials Security (Github-Secrets)
- AWS - RDS, Lambda, CloudWatch, S3
- Terraform - AWS Deployment (Infrastructure as Code)
- Python 3.12 - Primary programming language
- Pytest - Test Driven Development (TDD)
- PostgreSQL - Relational Database Management


## 🏛️ Architecture
- Two S3 buckets (one for ingested data and one for processed data). Both buckets are structured and well-organised so that data is easy to find.
- The Python application continually ingests all tables from the `totesys` database and stores the injested data in a json format. The application also:
  - operates automatically on a schedule
  - logs progress to Cloudwatch
  - triggers email alerts in the event of failures
  - follows good security practices (for example, preventing SQL injection and maintaining password security)
- The Python application remodels the data into a predefined schema suitable for a data warehouse and stores the data in Parquet format. The application also:
  - triggers automatically when it detects the completion of an ingested data job
  - adequately logs and monitors
  - populates the dimension and fact tables of a single "star" schema in the warehouse.
- The Python application loads the data into a prepared data warehouse at intervals. This is also logged and monitored.
- Includes a visual presentation that allows users to view useful data in the warehouse.

All Python code is thoroughly tested, PEP8 compliant, and tested for security vulnerabilities with the `pip-audit` and `bandit` packages. Test coverage exceeds 90%.

The project is deployed automatically using infrastucture-as-code (Terraform) and CI/CD (Github-Actions).

Changes to the source database will be reflected in the data warehouse within 30 minutes.

## 📊 The Data

The primary data source for the project is a database called `totesys` which is meant to simulate the back-end data of a commercial application. Data is inserted and updated into this database several times a day.


The data is remodelled into three overlapping star schemas. You can find the ERDs for these star schemas:

- ["Sales" schema](https://dbdiagram.io/d/637a423fc9abfc611173f637)
- ["Purchases" schema](https://dbdiagram.io/d/637b3e8bc9abfc61117419ee)
- ["Payments" schema](https://dbdiagram.io/d/637b41a5c9abfc6111741ae8)

The overall structure of the resulting data warehouse is shown [here](https://dbdiagram.io/d/63a19c5399cb1f3b55a27eca).

## 👀 Visualisation

We have created a BI dashboard to visualise some data insights:

- TODO

## 📁 File Structure

```
FSCIFA-project
├── .github
│   ├── workflows
│       ├── ci.yml        # CI/CD Automated deployment via Github Actions
├── src/                  # Source code for ETL/ELT
├── data/                 # Sample data or data schemas
├── tests/                # Unit and integration tests
├── terraform/            # AWS Deployment
├── Makefile              # Automated environment setup & configuration
├── mvp.png               # Illustration of expected minimum viable product
├── README.md             # Project overview
└── requirements.txt      # Third party Python modules
```

## 🚀 Setup & Deployment

TODO

