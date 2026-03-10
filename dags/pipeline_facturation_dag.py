from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
from pathlib import Path

# Chemins adaptés à l'environnement Docker
DATA_PATH = "/opt/airflow/data"
INPUT_FILE = f"{DATA_PATH}/factures_brutes.csv"
OUTPUT_FILE = f"{DATA_PATH}/factures_propres.csv"
LOG_FILE = f"{DATA_PATH}/pipeline.log"

# Fonction pour charger les données
def load_data(filepath):
    return pd.read_csv(filepath, parse_dates=['date_facture'])

# Fonction de nettoyage (adaptée pour Airflow)
def clean_data():
    # Charger les données
    df = load_data(INPUT_FILE)

    # Configuration des logs
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Log initial count of duplicates
    initial_duplicates = df.duplicated(subset=['id_facture'], keep=False).sum()
    logging.info(f"Début du nettoyage: {initial_duplicates} doublons initiaux détectés sur id_facture.")

    # 1. Suppression des montants HT invalides
    montants_invalides = df[df['montant_HT'].isna() | (df['montant_HT'] < 0)]
    df = df[(df['montant_HT'].notna()) & (df['montant_HT'] >= 0)]
    logging.info(f"Suppression de {len(montants_invalides)} lignes avec montants invalides.")

    # 2. Dates manquantes
    df['date_facture'] = df['date_facture'].replace(r'^\s*$', pd.NaT, regex=True)
    df['date_facture'] = df['date_facture'].fillna(pd.Timestamp(datetime.now().date()))
    logging.info(f"Remplacement des dates manquantes par la date du jour.")

    # 3. Suppression des taux_TVA manquants
    taux_tva_missing_count = df['taux_TVA'].isna().sum()
    df = df[df['taux_TVA'].notna()]
    logging.info(f"Suppression de {taux_tva_missing_count} lignes avec taux_TVA non renseigné.")

    # 4. Normalisation des codes d'actes
    df['code_acte'] = df['code_acte'].str.replace('-', '').str.upper()
    logging.info(f"Normalisation des codes d'actes. Nombre d'actes uniques: {df['code_acte'].nunique()}.")

    # 5. Suppression des doublons
    doublons = df.duplicated(subset=['id_facture'], keep='first').sum()
    df = df.drop_duplicates(subset=['id_facture'], keep='first')
    logging.info(f"Suppression de {doublons} doublons sur id_facture.")

    # 6. Calcul du montant_TTC
    df['montant_TTC'] = (df['montant_HT'] * (1 + df['taux_TVA'])).round(2)

    # 7. Validation
    assert all(df['montant_TTC'] > df['montant_HT']), "Erreur: montant_TTC <= montant_HT pour certaines lignes."
    logging.info("Validation réussie: montant_TTC > montant_HT.")

    # Sauvegarde du résultat
    df.to_csv(OUTPUT_FILE, index=False)
    logging.info(f"Pipeline terminé. Résultat sauvegardé dans {OUTPUT_FILE}.")

# Définition du DAG
default_args = {
    'owner': 'david',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_facturation',
    default_args=default_args,
    description='Pipeline de nettoyage des données de facturation EEG/Neuro',
    schedule_interval=None,
    catchup=False,
)

# Tâche de nettoyage
clean_task = PythonOperator(
    task_id='nettoyage_donnees_facturation',
    python_callable=clean_data,  # Appelle la fonction SANS argument
    dag=dag,
)
