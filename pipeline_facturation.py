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

    # Log initial count of duplicates in the incoming DataFrame
    initial_duplicates = df.duplicated(subset=['id_facture'], keep=False).sum()
    logging.info(f"Début du nettoyage: {initial_duplicates} doublons initiaux détectés sur id_facture avant tout traitement.")

    # 1. Compter les montants négatifs/manquants AVANT suppression
    montants_invalides = df[df['montant_HT'].isna() | (df['montant_HT'] < 0)]
    logging.info(f"Suppression de {len(montants_invalides)} lignes avec montants invalides.")

    # 2. Dates manquantes
    # Convertir explicitement les chaînes vides ou avec seulement des espaces en pd.NaT
    df['date_facture'] = df['date_facture'].replace(r'^\s*$', pd.NaT, regex=True)
    date_facturation_empty = df[df['date_facture'].isna()]
    logging.info(f"Nombre de lignes avec date_facture vide : {len(date_facturation_empty)}")
    df['date_facture'] = df['date_facture'].fillna(pd.Timestamp(datetime.now().date()))

    # 3. Suppression des montants HT invalides
    montant_HT_invalid = df[df['montant_HT'].isna() | (df['montant_HT'] < 0)]
    df = df[(df['montant_HT'].notna()) & (df['montant_HT'] >= 0)]
    logging.info(f"Suppression de {len(montant_HT_invalid)} lignes avec montants HT invalides.")

    # Suppression des taux_TVA non renseigner et mis à jour des logs
    taux_tva_missing_count = df['taux_TVA'].isna().sum() # Compter avant la suppression
    df = df[df['taux_TVA'].notna()]
    logging.info(f"Suppression de {taux_tva_missing_count} lignes avec taux_TVA non renseigner.")

    # 4. Normalisation des codes d'actes
    df['code_acte'] = df['code_acte'].str.replace('-', '').str.upper()
    logging.info(
        f"Normalisation des codes d'actes en majuscules et sans tirets. Nombre total d'actes uniques après normalisation : {df['code_acte'].nunique()}.")

    # 5. Suppression des doublons qui pourrait rester normalement est à 0
    doublons = df.duplicated(subset=['id_facture'], keep='first').sum()
    df = df.drop_duplicates(subset=['id_facture'], keep='first')
    logging.info(f"Suppression de {doublons} doublons sur id_facture.")

    # 6. Ajout d'une colonne calcul du montant_TTC
    df['montant_TTC'] = df['montant_HT'] * (1 + df['taux_TVA']).round(2)

    # 7. Validation : montant_TTC doit être > montant_HT
    assert all(df['montant_TTC'] > df['montant_HT']), "Erreur : montant_TTC <= montant_HT pour certaines lignes."
    logging.info("Validation réussie : montant_TTC > montant_HT pour toutes les lignes.")

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