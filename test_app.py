import unittest
from unittest.mock import MagicMock, patch
import app
import pytest

class TestApp(unittest.TestCase):
    @patch('app.storage_client')
    def test_check_if_bucket_exists_bucket_exists(self, mock_storage_client):
        mock_bucket = MagicMock()
        mock_storage_client.get_bucket.return_value = mock_bucket

        with patch('builtins.print') as mock_print:
            app.check_if_bucket_exists()

        mock_storage_client.get_bucket.assert_called_once_with('vmo2_bucket')
        mock_print.assert_called_once_with('Bucket vmo2_bucket exists')

# Test create_bucket funciton
    @patch('app.storage_client')
    def test_create_bucket(self, mock_storage_client):
        mock_bucket = MagicMock()
        mock_bucket.name = 'vmo2_bucket'
        mock_storage_client.create_bucket.return_value = mock_bucket

        with patch('builtins.print') as mock_print:
            app.create_bucket()

        mock_storage_client.create_bucket.assert_called_once_with('vmo2_bucket')
        mock_print.assert_called_once_with('Bucket vmo2_bucket created')


    # Test upload_to_gcs() fucntion
    @patch('app.storage_client')
    def test_upload_to_gcs(self, mock_storage_client):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.bucket.return_value = mock_bucket

        with patch('builtins.print') as mock_print:
            app.upload_to_gcs('vmo2_bucket', 'output.csv', 'output.csv')

        mock_storage_client.bucket.assert_called_once_with('vmo2_bucket')
        mock_bucket.blob.assert_called_once_with('output.csv')
        mock_blob.upload_from_filename.assert_called_once_with('output.csv')
        mock_print.assert_called_once_with('File output.csv uploaded to output.csv')



    # Test create_csv_file() function
    @patch('app.pd')
    def test_create_csv_file(self, mock_pd):
        mock_weather_data = {'temperature': 25, 'humidity': 70}
        mock_df = MagicMock()
        mock_pd.DataFrame.return_value = mock_df

        with patch('builtins.print') as mock_print:
            app.create_csv_file(mock_weather_data)

        mock_df.to_csv.assert_called_once_with('output.csv', index=True)

    # Test check_if_dataset_exists
    @patch('app.client')
    def test_check_if_dataset_exists(self, mock_client):
        mock_dataset = MagicMock()
        mock_client.get_dataset.return_value = mock_dataset

        with patch('builtins.print') as mock_print:
            app.check_if_dataset_exists()

        mock_client.get_dataset.assert_called_once_with(app.dataset_id)
        mock_print.assert_called_once_with(f'Dataset {app.dataset_id} exists')

    # Test create_dataset() function
    @patch('app.client')
    def test_create_dataset(self, mock_client):
        mock_dataset = MagicMock()
        mock_client.create_dataset.return_value = mock_dataset
        mock_dataset.dataset_id = 'VMO2_Dataset'  

        with patch('builtins.print') as mock_print, patch('google.cloud.bigquery.Dataset') as mock_bigquery_dataset:
            mock_bigquery_dataset.return_value = mock_dataset
            app.create_dataset()

            mock_bigquery_dataset.assert_called_once_with("VMO2_Dataset")
            mock_client.create_dataset.assert_called_once_with(mock_dataset, timeout=30)
            mock_print.assert_called_once_with('Dataset VMO2_Dataset created')


    # Test index() function
    @patch('app.check_if_bucket_exists')
    @patch('app.check_if_dataset_exists')
    def test_index(self, mock_check_if_dataset_exists, mock_check_if_bucket_exists):
        tester = app.app.test_client()
        response = tester.get('/')
        self.assertEqual(response.status_code, 200)
        mock_check_if_bucket_exists.assert_called_once()
        mock_check_if_dataset_exists.assert_called_once()
        self.assertIn(b'<h1>VMO2 Onboarding Project</h1>', response.data)


    # Test errpr html page
    def test_error(self):
        tester = app.app.test_client()
        response = tester.get('/error?message=TestError')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'TestError', response.data)

    # Test get_weather_for_user() function
    def test_get_weather_for_user(self):
            tester = app.app.test_client()
            with tester.session_transaction() as session:
                session['weather_data'] = {
                    'clouds': {'all': 75},
                    'weather': [{'description': 'clear sky'}],
                    'wind': {'speed': 5.5},
                    'name': 'TestLocation',
                    'main': {'feels_like': 300}
                }

            response = tester.get('/get_weather_for_user')
            self.assertEqual(response.status_code, 200)
            self.assertIn(b'TestLocation', response.data)
            self.assertIn(b'clear sky', response.data)
            self.assertIn(b'5.5', response.data)
            self.assertIn(b'26.85', response.data)  # 300K = 26.85C

    

if __name__ == '__main__':
    unittest.main()