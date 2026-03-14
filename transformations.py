def clean_data(raw_data):
    """
    Remove registros nulos e normaliza nomes de campos.
    Etapa essencial para garantir a consistência antes da análise.
    """
    cleaned = []
    for item in raw_data:
        # Filtra registros que não possuem campos essenciais
        if item.get("id") and item.get("value") is not None:
            cleaned.append({
                "record_id": item["id"],
                "normalized_value": float(item["value"]),
                "category": item.get("category", "unassigned").lower()
            })
    return cleaned

def aggregate_by_category(data):
    """
    Soma os valores normalizados agrupando por categoria.
    Útil para relatórios gerenciais e dashboards.
    """
    totals = {}
    for entry in data:
        cat = entry["category"]
        totals[cat] = totals.get(cat, 0) + entry["normalized_value"]
    return totals

def validate_schema(data):
    """
    Verifica se a estrutura mínima de dados está presente.
    Retorna True se válido, False caso contrário.
    """
    if not data:
        return False
    
    required_keys = {"id", "value"}
    return all(required_keys.issubset(item.keys()) for item in data)
