import os
import zipfile
import gdown
from cnnClassifier import logger
from cnnClassifier.utils.common import get_size
from cnnClassifier.entity.config_entity import DataIngestionConfig

class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def download_file(self) -> str:
        '''
        Fetch data from the source URL to local zip file
        '''
        try:
            # We use the raw ID for better reliability with gdown
            file_id = "1ZyETALkUeNhVp1h8ex0saR8C-zFY5Xjg"
            zip_download_dir = self.config.local_data_file
            
            logger.info(f"Downloading file ID: {file_id} to: {zip_download_dir}")

            if not os.path.exists(zip_download_dir):
                gdown.download(
                    id=file_id, # Using 'id' instead of 'url' is more stable
                    output=zip_download_dir, 
                    quiet=False,
                    fuzzy=True
                )
                logger.info(f"Downloaded data to {zip_download_dir}")
            else:
                logger.info(f"File already exists at {zip_download_dir}. Skipping download.")

        except Exception as e:
            raise e

    def extract_zip_file(self) -> None:
        '''
        Extract zip file to the unzip directory
        '''
        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)