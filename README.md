# 🧠 Synthetic Healthcare Data Pipeline (Medallion Architecture)

Projeto de engenharia de dados construído com arquitetura Medallion (Bronze → Silver → Gold) utilizando dados sintéticos do Synthea.

O pipeline realiza ingestão, transformação, validação e agregação de dados clínicos utilizando Python, Pandas e PostgreSQL.

## 🏗️ Arquitetura do Projeto

![Arquitetura do Pipeline](docs/architecture_pipeline.png)

## 🎯 Objetivo

Construir um pipeline ETL completo simulando um cenário real de engenharia de dados:

- Ingestão de dados clínicos em formato CSV
- Organização em camadas Medallion
- Transformações e validações de qualidade
- Modelagem analítica (Gold Layer)

## 🧰 Tecnologias

- Python
- Pandas
- SQLAlchemy
- PostgreSQL
- Jupyter Notebook
- Git & GitHub (workflow com branches)

## 🧱 Arquitetura Medallion

### 🥉 Bronze Layer — Ingestão

Script:

- `1_bronze_layer_construction.py`

Responsável por:

- Carregar CSVs do Synthea
- Inserir dados brutos no PostgreSQL

Tabelas:

- bronze_patients
- bronze_encounters
- bronze_conditions

---

### 🥈 Silver Layer — Limpeza e Padronização

Script:

- `2_silver_layer_construction.py`

Transformações:

- criação de full_name
- cálculo de duration_hours
- normalização de campos
- checks de qualidade

Tabelas:

- silver_patients
- silver_encounters
- silver_conditions

---

### 🥇 Gold Layer — Modelagem Analítica

Script:

- `3_gold_layer_construction.py`

Saídas:

- gold_obt_encounters (One Big Table)
- gold_patient_summary
- gold_encounter_summary

## ✅ Data Quality

Validações aplicadas:

- checagem de nulos em colunas críticas
- validação de valores negativos
- verificação de integridade antes da carga

Notebook:

- `medical_data_verification.ipynb`

## ▶️ Como executar

Clone o repositório:

```bash
git clone <repo>
cd ed_prjt1
```

Instale as dependências:
```bash
pip install -r requirements.txt
```

Configure as variáveis de ambiente (crie .env):
```bash
PG_USER=
PG_PASS=
PG_HOST=localhost
PG_PORT=5432
PG_DB=
```

Execute:
```bash
python scripts/aula_1_banco/1_bronze_layer_construction.py
python scripts/aula_1_banco/2_silver_layer_construction.py
python scripts/aula_1_banco/3_gold_layer_construction.py
```


---

## 📂 8️⃣ Estrutura do projeto

```md
## 📁 Estrutura

ed_prjt1/
│
├── data/
├── scripts/
│ └── aula_1_banco/
│ ├── bronze
│ ├── silver
│ ├── gold
│
├── docs/
├── README.md
```