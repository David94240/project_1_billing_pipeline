from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import logging

DATA_PATH = "/opt/airflow/data"
INPUT_FILE = f"{DATA_PATH}/factures_brutes.csv"
OUTPUT_FILE = f"{DATA_PATH}/factures_propres.csv"


@dag(
    dag_id="pipeline_facturation_task",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["facturation", "data_pipeline"]
)

def pipeline_facturation_task():
    @task
    def extract_data():
        # Chargement des données :
        df = pd.read_csv(INPUT_FILE)

        logging.info(f"{len(df)} lignes chargées")

        # Sauvegarde dans la variable temp_file pour gérérer le fichier factures_raw.parquet dans /data
        temp_file = f"{DATA_PATH}/factures_raw.parquet"

        df.to_parquet(temp_file)

        # -------------------------RAW / BRONZE layer-------------------------
        # Observer les données brutes
        df.dtypes
        df.info()

        logging.info(df['date_facture'].dtype)
        logging.info(df['date_facture'].head(10))

        return temp_file

    @task
    def clean_data(file_path):
        df = pd.read_parquet(file_path)
        # -------------------------SILVER layer (data cleaning)-------------------------
        # Dates manquantes et remplacement des caractères speciaux :
        df['date_facture'] = pd.to_datetime(df['date_facture'], errors="coerce")

        df['date_facture'] = df['date_facture'].fillna(
            pd.Timestamp(datetime.now().date())
        )

        logging.info("Remplacement des dates manquantes par la date du jour.")

        # Suppression des montants HT invalides et Suppression des taux_TVA manquants :
        df = df[(df['montant_HT'].notna()) & (df['montant_HT'] >= 0)]
        df['montant_HT'] = pd.to_numeric(df['montant_HT'], errors="coerce")
        df = df[df['taux_TVA'].notna()]
        df['taux_TVA'] = pd.to_numeric(df['taux_TVA'], errors="coerce")

        # Normalisation des codes d'actes :
        df['code_acte'] = df['code_acte'].str.replace('-', '').str.upper()

        # Suppression des doublons :
        df = df.drop_duplicates(subset=['id_facture'], keep='first')

        # Calcul du montant_TTC :
        # df["taux_TVA"] = pd.to_numeric(df["taux_TVA"], errors="coerce") (déjà fait ligne 59)
        # normalisation si taux en %
        df.loc[df["taux_TVA"] > 1, "taux_TVA"] = df["taux_TVA"] / 100
        df["montant_TTC"] = df["montant_HT"] * (1 + df["taux_TVA"])

        # Chargement des données modifié dans le "output" :
        output = f"{DATA_PATH}/factures_clean.parquet"

        df.to_parquet(output)

        # Voir les transformation de données :
        df.info()
        df.dtypes

        return output

    @task
    # Validation des données :
    def validate_data(file_path):
        df = pd.read_parquet(file_path)

        if not all(df['montant_TTC'] > df['montant_HT']):
            raise ValueError("Validation échouée")

        return file_path

    @task
    # Sauvegarde des données :
    def save_data(file_path):
        df = pd.read_parquet(file_path)

        df.to_csv(OUTPUT_FILE, index=False)

        logging.info("Sauvegarde terminée")


    # Orchestration
    raw = extract_data()
    cleaned = clean_data(raw)
    validated = validate_data(cleaned)
    save_data(validated)


pipeline_facturation_task()