import pandas as pd
import re
import logging

# Słownik znanych marek - łatwo tu dopisać kolejne
BRAND_MAPPING = {
    'Lavaz': 'Lavazza',
    'Prima': 'Prima',
    'Mlekovit': 'Mlekovita',
    'Tymbark': 'Tymbark',
    'Jacob': 'Jacobs',
    'Lipton': 'Lipton'
}

def identify_brand(product_name):
    """Przeszukuje nazwę produktu pod kątem znanych marek."""
    for key, brand_value in BRAND_MAPPING.items():
        if key.lower() in product_name.lower():
            return brand_value
    return 'Unknown'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def normalize_text(text):
    if pd.isna(text):
        return "Unknown"
    text = str(text).strip().lower()
    text = re.sub(r'\s+', ' ', text)
    return text.title()

def extract_weight(product_name):
    match = re.search(r'([\d\.,]+)\s*(kg|g|l|ml)', product_name.lower())
    if match:
        value_str = match.group(1).replace(',', '.')
        unit = match.group(2)
        try:
            value = float(value_str)
            if unit == 'kg':
                return f"{int(value * 1000)}g"
            elif unit == 'l':
                return f"{int(value * 1000)}ml"
            else:
                return f"{int(value)}{unit}"
        except ValueError:
            return None
    return "No Data"

def run_etl():
    logging.info("Rozpoczęcie procesu ETL...")
    
    # 1. EXTRACT
    try:
        
        df = pd.read_csv('../data/raw_data.csv', on_bad_lines='skip', sep=None, engine='python')
        logging.info(f"Wczytano {len(df)} rekordów (pominiecie uszkodzonych linii).")
    except FileNotFoundError:
        logging.error("Nie znaleziono pliku źródłowego.")
        return

    logging.info("Czyszczenie nazw i kategorii...")
    df['clean_name'] = df['product_name'].apply(normalize_text)
    df['clean_category'] = df['category'].apply(normalize_text)
    
    logging.info("Ekstrakcja i normalizacja gramatury...")
    df['standardized_weight'] = df['product_name'].apply(extract_weight)
    
    quarantine_df = df[df['standardized_weight'] == 'No Data']
    clean_df = df[df['standardized_weight'] != 'No Data']

    clean_df.to_csv('../data/clean_data.csv', index=False)
    quarantine_df.to_csv('../data/quarantine_report.csv', index=False)
    
    logging.info(f"Zakończono. Zapisano {len(clean_df)} czystych rekordów i {len(quarantine_df)} błędnych.")

if __name__ == "__main__":
    run_etl()
