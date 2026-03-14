import json

def process_dataset_file(file_path):
    """
    Lê o arquivo persistido e aplica as transformações.
    """
    # Exemplo tratando especificamente JSON para as transformações existentes
    if file_path.endswith('.json'):
        with open(file_path, 'r') as f:
            data = json.load(f)
            return clean_data(data)
    return f"Arquivo {file_path} pronto para processamento binário/específico."

def clean_data(raw_data):
    """
    Limpa dados JSON normalizando campos.
    """
    cleaned = []
    if not isinstance(raw_data, list): return cleaned
    
    for item in raw_data:
        if item.get("id") and item.get("value") is not None:
            cleaned.append({
                "record_id": item["id"],
                "normalized_value": float(item["value"]),
                "category": item.get("category", "unassigned").lower()
            })
    return cleaned
