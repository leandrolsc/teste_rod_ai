import unittest
from unittest.mock import patch, MagicMock
from api_client import DatasetClient
from transformations import clean_data, validate_schema

class TestDatasetProject(unittest.TestCase):

    def setUp(self):
        self.client = DatasetClient("https://api.exemplo.com", "token_fake")

    @patch('requests.get')
    def test_get_dataset_success(self, mock_get):
        """Simula uma resposta de sucesso da API."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": 1, "value": 10.5, "category": "A"},
            {"id": 2, "value": 20.0, "category": "B"}
        ]
        mock_get.return_value = mock_response

        result = self.client.get_dataset("proj_123")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)

    @patch('requests.get')
    def test_get_dataset_http_error(self, mock_get):
        """Valida o comportamento em caso de erro 404."""
        mock_get.return_value.raise_for_status.side_effect = Exception("404 Not Found")
        
        with self.assertRaises(Exception):
            self.client.get_dataset("proj_invalido")

    def test_transformations_logic(self):
        """Valida as funções de limpeza e transformação."""
        raw = [
            {"id": "001", "value": "100", "category": "Teste"},
            {"id": "002", "value": None}, # Deve ser filtrado
            {"id": None, "value": 50}      # Deve ser filtrado
        ]
        
        cleaned = clean_data(raw)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["normalized_value"], 100.0)
        self.assertEqual(cleaned[0]["category"], "teste")

    def test_schema_validation(self):
        """Valida a detecção de schemas corrompidos."""
        invalid_data = [{"wrong_key": 1}]
        self.assertFalse(validate_schema(invalid_data))
        
        valid_data = [{"id": 1, "value": 10}]
        self.assertTrue(validate_schema(valid_data))

if __name__ == '__main__':
    unittest.main()
