import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import numpy as np
import psycopg2
from io import StringIO

BASE_DIR = os.path.dirname(__file__)
FILES = {
    "bronze_patients": os.path.join(BASE_DIR, "../../data/aula_2_banco_de_dados/patients.csv"),
    "bronze_encounters": os.path.join(BASE_DIR, "../../data/aula_2_banco_de_dados/encounters.csv"),
    "bronze_conditions": os.path.join(BASE_DIR, "../../data/aula_2_banco_de_dados/conditions.csv"),
}

def get_conn(credentials):
    """Retorna uma conexão psycopg2 pura."""
    try:
        return psycopg2.connect(
            host=credentials["PG_HOST"],
            port=credentials["PG_PORT"],
            dbname=credentials["PG_DB"],
            user=credentials["PG_USER"],
            password=credentials["PG_PASS"],
        )
    except Exception as e:
        print(f"Erro ao conectar: {e}")
        return None

def get_engine(credentials):
    """Retorna engine SQLAlchemy (usado apenas para read_sql)."""
    try:
        url = (f"postgresql+psycopg2://{credentials['PG_USER']}:{credentials['PG_PASS']}"
               f"@{credentials['PG_HOST']}:{credentials['PG_PORT']}/{credentials['PG_DB']}")
        return create_engine(url, pool_pre_ping=True, echo=False)
    except Exception as e:
        print(f"Erro ao criar engine: {e}")
        return None

def df_to_postgres(df, table_name, conn, if_exists="replace"):
    """
    Carrega um DataFrame no PostgreSQL usando psycopg2 puro via COPY.
    Compatível com pandas 3.x sem depender do SQLAlchemy para escrita.
    """
    cursor = conn.cursor()

    if if_exists == "replace":
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')

    # Cria a tabela com base nas colunas do DataFrame
    cols = []
    for col, dtype in df.dtypes.items():
        if "int" in str(dtype):
            pg_type = "BIGINT"
        elif "float" in str(dtype):
            pg_type = "DOUBLE PRECISION"
        elif "datetime" in str(dtype):
            pg_type = "TIMESTAMP"
        else:
            pg_type = "TEXT"
        cols.append(f'"{col}" {pg_type}')

    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)})'
    cursor.execute(create_sql)

    # Usa COPY para inserção rápida
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)
    cursor.copy_expert(
        f'COPY "{table_name}" FROM STDIN WITH CSV NULL \'\\N\'',
        buffer
    )

    conn.commit()
    cursor.close()
    print(f"Tabela '{table_name}' carregada com sucesso ({len(df)} linhas).")

def sql_to_df(query, pg_conn):
    """Lê dados via psycopg2 puro, compatível com pandas 3.x."""
    return pd.read_sql(query, con=pg_conn)


def bronze_layer_construction(credentials):

    conn = get_conn(credentials)
    if conn is None:
        print("Processo abortado.")
        return

    print(credentials["PG_USER"])
    print(credentials["PG_HOST"])

    for table_name, fname in FILES.items():
        try:
            print(f"Carregando '{fname}' para '{table_name}'...")
            df = pd.read_csv(fname, low_memory=False)

            if df.empty:
                print(f"DataFrame vazio para {fname}. Pulando.")
                continue

            df['execution_date'] = datetime.today().strftime('%Y-%m-%d')
            df_to_postgres(df, table_name, conn)

        except Exception as e:
            print(f"Erro no arquivo {fname}: {e}")
            continue

    conn.close()
    print("\nCarga bronze concluída.")


def silver_layer_construction(credentials):

    def transform_patients(df):
        print("Transformando pacientes...")
        cols = [
            "id", "birthdate", "gender", "race", "ethnicity",
            "first", "middle", "last", "deathdate",
            "healthcare_expenses", "healthcare_coverage", "income"
        ]
        patients = df[cols].copy()
        patients["full_name"] = (
            patients["first"].fillna("") + " " +
            patients["middle"].fillna("") + " " +
            patients["last"].fillna("")
        ).str.strip().replace(r"\s+", " ", regex=True)
        patients["death"] = np.where(patients["deathdate"].notna(), "dead", "alive")
        patients["coverage_minus_expenses"] = (
            patients["healthcare_coverage"].fillna(0) - patients["healthcare_expenses"].fillna(0)
        )
        patients["over_expenses"] = np.where(patients["coverage_minus_expenses"] < 0, 1, 0)
        patients["income"] = patients["income"].fillna(0)
        return patients.drop(columns=["first", "middle", "last"])

    def transform_encounters(df):
        print("Transformando encontros...")
        cols = [
            "id", "start", "stop", "patient", "encounterclass", "description",
            "base_encounter_cost", "total_claim_cost", "payer_coverage", "reasondescription"
        ]
        encounters = df[cols].copy()
        encounters = encounters.dropna(subset=["id", "patient"])
        encounters["start"] = pd.to_datetime(encounters["start"], errors="coerce")
        encounters["stop"] = pd.to_datetime(encounters["stop"], errors="coerce")
        encounters["duration_hours"] = (
            (encounters["stop"] - encounters["start"]).dt.total_seconds() / 3600
        )
        return encounters

    def transform_conditions(df):
        print("Transformando condições...")
        cols = ["start", "stop", "patient", "description"]
        conditions = df[cols].copy()
        conditions["condition"] = conditions["description"].str.replace(r"\s*\(.*\)", "", regex=True).str.strip()
        conditions["condition_type"] = conditions["description"].str.extract(r"\((.*?)\)")
        return conditions

    conn = get_conn(credentials)
    if conn is None:
        return

    try:
        print("Lendo camada bronze...")
        patients = sql_to_df("SELECT * FROM bronze_patients", conn)
        encounters = sql_to_df("SELECT * FROM bronze_encounters", conn)
        conditions = sql_to_df("SELECT * FROM bronze_conditions", conn)
        print("Extração bronze concluída.")

        patients.columns = patients.columns.str.strip().str.lower()
        encounters.columns = encounters.columns.str.strip().str.lower()
        conditions.columns = conditions.columns.str.strip().str.lower()

        patients_clean = transform_patients(patients)
        encounters_clean = transform_encounters(encounters)
        conditions_clean = transform_conditions(conditions)

        print("\nCarregando camada silver...")
        df_to_postgres(patients_clean, "silver_patients", conn)
        df_to_postgres(encounters_clean, "silver_encounters", conn)
        df_to_postgres(conditions_clean, "silver_conditions", conn)
        print("\nCamada silver concluída.")

    except Exception as e:
        print(f"Erro na tarefa silver: {e}")
    finally:
        conn.close()


def gold_layer_construction(credentials):

    def create_one_big_table(patients_df, encounters_df):
        print("Criando OBT...")
        obt = encounters_df.merge(
            patients_df, left_on="patient", right_on="id",
            how="left", suffixes=("_encounter", "_patient")
        )
        obt = obt.rename(columns={
            "id_encounter": "encounter_id",
            "patient": "patient_id",
            "start": "encounter_start_date",
            "stop": "encounter_end_date",
            "description": "encounter_description",
            "id_patient": "patient_original_id"
        })
        cols = [
            "encounter_id", "patient_id", "encounter_start_date", "encounter_end_date",
            "encounterclass", "encounter_description", "duration_hours",
            "total_claim_cost", "payer_coverage", "gender", "race", "ethnicity", "full_name"
        ]
        return obt[cols]

    def create_patient_summary(patients_df, encounters_df):
        print("Criando resumo por paciente...")
        agg = encounters_df.groupby('patient').agg(
            total_encounters=('id', 'count'),
            total_claim_cost=('total_claim_cost', 'sum'),
            avg_encounter_duration_hours=('duration_hours', 'mean')
        ).reset_index().rename(columns={'patient': 'id'})
        summary = patients_df.merge(agg, on='id', how='left')
        return summary.rename(columns={'id': 'patient_id'}).fillna(0)

    conn = get_conn(credentials)
    if conn is None:
        return

    try:
        print("\nLendo camada silver...")
        patients = sql_to_df("SELECT * FROM silver_patients", conn)
        encounters = sql_to_df("SELECT * FROM silver_encounters", conn)
        conditions = sql_to_df("SELECT * FROM silver_conditions", conn)
        print("\nExtração silver concluída.")

        obt_df = create_one_big_table(patients, encounters)
        summary_df = create_patient_summary(patients, encounters)

        print("\nCarregando camada gold...")
        df_to_postgres(obt_df, "gold_obt_encounters", conn)
        df_to_postgres(summary_df, "gold_patient_summary", conn)
        print("\nCamada gold concluída.")

    except Exception as e:
        print(f"Erro na tarefa gold: {e}")
    finally:
        conn.close()


def print_erro(context):
    task_id = context.get("task_instance").task_id
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    print("⚠️ ERRO DETECTADO ⚠️")
    print(f"DAG: {dag_id}")
    print(f"Task: {task_id}")
    print(f"Data de execução: {execution_date}")
    print(f"Exceção: {exception}")