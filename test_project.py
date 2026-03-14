import unittest
import os
from unittest.mock import patch, MagicMock
from api_client import DatasetClient

class TestDatasetDownload(unittest.TestCase):

    def setUp(self):
        self.client = DatasetClient("https://api.test.com", "valid_token")

    @patch('requests.get')
    def test_correct_url_and_format_param(self, mock_get):
        """Verifica se o parâmetro ?format=parquet é enviado corretamente."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b"dummy data"]
        mock_get.return_value = mock_response

        # Executa o download pedindo parquet
        file_path = self.client.get_dataset("proj123", file_format="parquet")

        # Verifica construção da URL e Query Params
        args, kwargs = mock_get.call_args
        self.assertEqual(kwargs['params']['format'], 'parquet')
        
        # Verifica persistência
        self.assertTrue(os.path.exists(file_path))
        self.assertTrue(file_path.endswith(".parquet"))
        
        # Cleanup
        os.remove(file_path)

    @patch('requests.get')
    def test_fallback_format_logic(self, mock_get):
        """Verifica se usa o formato padrão caso um formato inválido seja passado."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b"{}"]
        mock_get.return_value = mock_response

        # Passa um formato não suportado "exe"
        self.client.get_dataset("proj123", file_format="exe")
        
        # Deve ter feito o fallback para o default (parquet)
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs['params']['format'], 'parquet')

    @patch('requests.get')
    def test_persistence_failure_cleanup(self, mock_get):
        """Verifica comportamento em erro 500."""
        mock_get.return_value.status_code = 500
        mock_get.return_value.raise_for_status.side_effect = Exception("Internal Server Error")

        with self.assertRaises(Exception):
            self.client.get_dataset("proj_error")

if __name__ == '__main__':
    unittest.main()
