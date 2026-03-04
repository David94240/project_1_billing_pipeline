import pandas as pd
from datetime import datetime
import logging

# Configuration des logs (une seule fois)
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_data(filepath):
    """Charge le CSV en DataFrame."""
    return pd.read_csv(filepath, parse_dates=['date_facture'])

def clean_data(df):
    """
    Nettoie les données de facturation.
    """
    # 1. Compter les montants négatifs/manquants AVANT suppression
    montants_invalides = df[df['montant_HT'].isna() | (df['montant_HT'] < 0)]
    logging.info(f"Suppression de {len(montants_invalides)} lignes avec montants invalides.")

    # 2. Dates manquantes
    df['date_facture'] = df['date_facture'].fillna(pd.Timestamp(datetime.now().date()))
    logging.info(f"Remplacement de {df['date_facture'].isna().sum()} dates manquantes par la date du jour.")

    # 3. Suppression des montants HT invalides
    df = df[(df['montant_HT'].notna()) & (df['montant_HT'] >= 0)]

    # 4. Normalisation des codes d'actes
    df['code_acte'] = df['code_acte'].str.replace('-', '').str.upper()

    # 5. Suppression des doublons
    doublons = df.duplicated(subset=['id_facture']).sum()
    df = df.drop_duplicates(subset=['id_facture'], keep='first')
    logging.info(f"Suppression de {doublons} doublons sur id_facture.")

    # 6. Ajout d'une colonne calcul du montant_TTC
    df['montant_TTC'] = df['montant_HT'] * (1 + df['taux_TVA']).round(2)

    return df

if __name__ == "__main__":
    input_file = "factures_brutes.csv"
    output_file = "factures_propres.csv"

    # Chargement et nettoyage
    df_brut = load_data(input_file)
    logging.info(f"Chargement de {len(df_brut)} lignes depuis {input_file}.")

    df_propre = clean_data(df_brut)
    df_propre.to_csv(output_file, index=False)
    logging.info(f"Pipeline terminé. Résultat sauvegardé dans {output_file}.")