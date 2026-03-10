from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import logging

DATA_PATH = "/opt/airflow/data"
INPUT_FILE = f"{DATA_PATH}/factures_brutes.csv"
OUTPUT_FILE = f"{DATA_PATH}/factures_propres.csv"


@dag(
    dag_id="pipeline_facturation",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["facturation", "data_pipeline"]
)

def pipeline_facturation():

    @task
    def extract_data():
        logging.info("Chargement du fichier CSV")

        df = pd.read_csv(INPUT_FILE)

        logging.info(f"{len(df)} lignes chargées")

        return df


    @task
    def clean_data(df):

        df['date_facture'] = df['date_facture'].replace(r'^\s*$', pd.NaT, regex=True)
        df['date_facture'] = df['date_facture'].fillna(pd.Timestamp(datetime.now().date()))

        df = df[(df['montant_HT'].notna()) & (df['montant_HT'] >= 0)]
        df = df[df['taux_TVA'].notna()]

        df['code_acte'] = df['code_acte'].str.replace('-', '').str.upper()

        df = df.drop_duplicates(subset=['id_facture'], keep='first')

        df['montant_TTC'] = df['montant_HT'] * (1 + df['taux_TVA'])

        logging.info("Nettoyage terminé")

        return df


    @task
    def validate_data(df):

        if not all(df['montant_TTC'] > df['montant_HT']):
            raise ValueError("Validation échouée : montant_TTC <= montant_HT")

        logging.info("Validation réussie")

        return df


    @task
    def save_data(df):

        df.to_csv(OUTPUT_FILE, index=False)

        logging.info(f"Données sauvegardées dans {OUTPUT_FILE}")


    # Orchestration
    raw = extract_data()
    cleaned = clean_data(raw)
    validated = validate_data(cleaned)
    save_data(validated)


pipeline_facturation()