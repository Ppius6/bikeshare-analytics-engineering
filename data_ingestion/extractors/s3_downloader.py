"""Download bike share data from S3 bucket."""

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class S3BikeShareDownloader:
    """Download bike share data from S3 bucket."""

    def __init__(
        self,
        base_url: str = "https://s3.amazonaws.com/tripdata",
        start_from: str = "JC-202501-citibike-tripdata.csv.zip",
        data_dir: str = "./data/raw",
        max_workers: int = 4,
    ):
        self.base_url = base_url
        self.start_from = start_from
        self.data_dir = Path(data_dir)
        self.max_workers = max_workers
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def get_file_links(self) -> List[str]:
        """Fetch and parse file links from S3 bucket."""
        try:
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "xml")
            keys = [
                key.text
                for key in soup.find_all("Key")
                if key.text.startswith("JC") and key.text.endswith(".zip")
            ]

            if self.start_from in keys:
                start_index = keys.index(self.start_from)
                file_list = keys[start_index:]
                logger.info(f"Found {len(file_list)} files to download")
                return file_list

            logger.warning(f"Start file '{self.start_from}' not found")
            return []

        except Exception as e:
            logger.error(f"Error fetching file links: {e}")
            raise

    def download_file(self, file_name: str) -> Optional[Path]:
        """Download a single file."""
        zip_path = self.data_dir / file_name
        csv_name = file_name.replace(".zip", ".csv")
        csv_path = self.data_dir / csv_name

        # Skip if CSV already exists
        if csv_path.exists():
            logger.info(f"CSV already exists: {csv_name}")
            return csv_path

        # Skip if ZIP already exists (will extract later)
        if zip_path.exists():
            logger.info(f"ZIP already exists: {file_name}")
            return zip_path

        try:
            url = f"{self.base_url}/{file_name}"
            logger.info(f"Downloading: {file_name}")

            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            logger.info(f"Downloaded: {file_name}")
            return zip_path

        except Exception as e:
            logger.error(f"Error downloading {file_name}: {e}")
            return None

    def extract_zip(self, zip_path: Path) -> Optional[Path]:
        """Extract ZIP file and return CSV path."""
        if not zip_path.exists():
            return None

        csv_name = zip_path.name.replace(".zip", ".csv")
        csv_path = self.data_dir / csv_name

        if csv_path.exists():
            logger.info(f"CSV already extracted: {csv_name}")
            zip_path.unlink()  # Remove ZIP after extraction
            return csv_path

        try:
            with ZipFile(zip_path, "r") as zip_ref:
                # Find CSV file in ZIP (skip __MACOSX folders)
                csv_files = [
                    name
                    for name in zip_ref.namelist()
                    if name.endswith(".csv") and not name.startswith("__MACOSX")
                ]

                if not csv_files:
                    logger.error(f"No CSV found in {zip_path.name}")
                    return None

                # Extract the CSV
                zip_ref.extract(csv_files[0], self.data_dir)
                extracted_path = self.data_dir / csv_files[0]

                # Rename if necessary
                if extracted_path != csv_path:
                    extracted_path.rename(csv_path)

                logger.info(f"Extracted: {csv_name}")

            # Remove ZIP file after successful extraction
            zip_path.unlink()
            return csv_path

        except Exception as e:
            logger.error(f"Error extracting {zip_path.name}: {e}")
            return None

    def download_all(self, limit: Optional[int] = None) -> List[Path]:
        """Download all files from S3 bucket."""
        file_links = self.get_file_links()

        if limit:
            file_links = file_links[:limit]
            logger.info(f"Limiting download to {limit} files")

        if not file_links:
            logger.warning("No files to download")
            return []

        # Download files in parallel
        csv_files = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            downloaded_paths = list(executor.map(self.download_file, file_links))

            # Extract ZIP files
            for path in downloaded_paths:
                if path and path.suffix == ".zip":
                    csv_path = self.extract_zip(path)
                    if csv_path:
                        csv_files.append(csv_path)
                elif path and path.suffix == ".csv":
                    csv_files.append(path)

        logger.info(f"Successfully processed {len(csv_files)} files")
        return csv_files


if __name__ == "__main__":
    # Test download
    downloader = S3BikeShareDownloader(data_dir=os.getenv("DATA_DIR", "./data/raw"))

    # Download only 2 files for testing
    files = downloader.download_all(limit=2)

    if files:
        logger.info(f"Downloaded {len(files)} files:")
        for f in files:
            logger.info(f"  - {f.name}")
    else:
        logger.error("No files downloaded")
