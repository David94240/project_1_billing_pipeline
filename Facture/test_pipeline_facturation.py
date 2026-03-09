import pandas as pd
import pytest
from pipeline_facturation import clean_data  # Import de ta fonction

# Fixture : crée un DataFrame de test réutilisable
@pytest.fixture
def sample_data():
    data = {
        'id_facture': ['FACT-0001', 'FACT-0002', 'FACT-0003'],
        'date_facture': ['2023-01-01', None, '2023-01-03'],
        'code_acte': ['EEG-001', 'NEURO-001', 'invalid-code'],
        'montant_HT': [100.0, -50.0, 200.0],
        'taux_TVA': [0.2, 0.1, None],
        'patient_id': ['PAT-0001', 'PAT-0002', 'PAT-0003']
    }
    return pd.DataFrame(data)

# Test 1 : Vérifie que les montants HT négatifs sont supprimés
def test_remove_negative_montant_HT(sample_data):
    df_clean = clean_data(sample_data)
    assert all(df_clean['montant_HT'] >= 0), "Des montants HT négatifs sont encore présents."

# Test 2 : Vérifie que montant_TTC > montant_HT
def test_montant_TTC_gt_montant_HT(sample_data):
    df_clean = clean_data(sample_data)
    assert all(df_clean['montant_TTC'] > df_clean['montant_HT']), "montant_TTC <= montant_HT pour certaines lignes."

# Test 3 : Vérifie la normalisation des codes d'actes
def test_normalize_code_acte(sample_data):
    df_clean = clean_data(sample_data)
    assert all(df_clean['code_acte'].str.contains('-') == False), "Certains codes contiennent encore des tirets."
    assert all(df_clean['code_acte'].str.isupper()), "Certains codes ne sont pas en majuscules."

# Test 4 : Vérifie que les doublons sur id_facture sont supprimés
# On le lance indépendament des autres avec la commande :
# pytest test_pipeline_facturation.py::test_remove_duplicates -v

def test_remove_duplicates():
    # Créer un DataFrame avec uniquement des lignes valides
    data = {
        'id_facture': ['FACT-0001', 'FACT-0002', 'FACT-0003'],
        'date_facture': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'code_acte': ['EEG001', 'NEURO001', 'EEG002'],
        'montant_HT': [100.0, 200.0, 300.0],  # Tous positifs
        'taux_TVA': [0.2, 0.1, 0.2],          # Tous renseignés
        'patient_id': ['PAT-0001', 'PAT-0002', 'PAT-0003']
    }
    df = pd.DataFrame(data)

    # Ajouter un doublon sur la première ligne
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)

    # Exécuter clean_data
    df_clean = clean_data(df)

    # Vérifier que le doublon a été supprimé
    assert len(df_clean) == len(df) - 1, "Le doublon n'a pas été supprimé."

# Test 5 : Vérifie que les dates manquantes sont remplacées
def test_fill_missing_dates(sample_data):
    df_clean = clean_data(sample_data)
    assert df_clean['date_facture'].isna().sum() == 0, "Des dates manquantes n'ont pas été remplacées."
