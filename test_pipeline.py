import pandas as pd
import sqlite3
from transform import transform_claims_data

def test_aggregation_logic_matches_sql():
    """
    Valida se a lógica do Pandas produz o mesmo resultado que o SQL de referência.
    """
    # Dados de entrada controlados (Mock)
    raw_data = {
        'timestamp': ['2023-10-01 10:00:00', '2023-10-01 15:00:00', '2023-10-02 09:00:00'],
        'tipo_sinistro': ['roubo', ' ROUBO ', 'colisao'],
        'valor': [100.0, 200.0, 500.0]
    }
    df_input = pd.DataFrame(raw_data)

    # Execução do Pipeline (Python)
    df_output = transform_claims_data(df_input)

    # Execução via SQL (Simulação de ambiente de validação)
    conn = sqlite3.connect(':memory:')
    df_input.to_sql('claims', conn, index=False)
    query = """
        SELECT 
            DATE(timestamp) as data_referencia, 
            UPPER(TRIM(tipo_sinistro)) as tipo_sinistro, 
            SUM(valor) as valor
        FROM claims
        GROUP BY 1, 2
    """
    df_sql = pd.read_sql(query, conn)

    # Assertivas (Validação de conformidade)
    # Comparamos as somas e o formato final
    assert len(df_output) == len(df_sql), "Quantidade de linhas agregadas diverge"
    assert df_output.loc[df_output['tipo_sinistro'] == 'ROUBO', 'valor'].values[0] == 300.0
    print("✅ Teste de agregação temporal e normalização passou com sucesso!")

if __name__ == "__main__":
    test_aggregation_logic_matches_sql()