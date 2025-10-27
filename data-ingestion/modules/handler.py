# handler.py

import logging as log
from typing import Dict, Any, List, Optional 

# Local imports
from modules.gcs import GCSClient

# --- Configuration Constants ---
SOURCE_BUCKET: str = "supply-chain-compensation-analysis-with-nlp"
SOURCE_FOLDER: str = "raw_data"
TARGET_FOLDER: str = "processed_data"


def handle_gcs_event_data(data: Dict[str, Any]) -> None:
    """
    Handles the core logic of the GCS event, including validation, read, and move.

    Args:
        data (Dict[str, Any]): The GCS event payload (e.g., {'bucket': '...', 'name': '...'}).
    """
    # 1. Extract bucket and blob names
    bucket_name: str = data.get("bucket", "")
    blob_name: str = data.get("name", "")
    
    # Extract folder path for validation and blob name (file name) for target path
    folder_path: str
    blob_path: str
    folder_path, blob_path = blob_name.rsplit('/', 1) if '/' in blob_name else ("", blob_name)

    log.info(f"Received event for file: gs://{bucket_name}/{blob_name}")
    
    # 2. Validation Checks
    
    # 2a. Validate the bucket name
    if bucket_name != SOURCE_BUCKET:
        log.info(f"Filtering: Ignoring event for bucket: {bucket_name}. Expected: {SOURCE_BUCKET}")
        return

    # 2b. Validate the folder/prefix
    log.debug(f"Folder path check: folder_path='{folder_path}', SOURCE_FOLDER='{SOURCE_FOLDER}'")
    if folder_path != SOURCE_FOLDER:
        log.info(f"Filtering: Ignoring event for blob name: {blob_name}. Expected prefix: {SOURCE_FOLDER}/")
        return

    log.info(f"Validated file: Processing gs://{bucket_name}/{blob_name}")

    # 3. Initialize GCS Client with event details as defaults
    gcs_client: GCSClient = GCSClient(bucket_name=bucket_name, blob_name=blob_name)
    
    # 4. Get the blob content
    blob_data: Optional[str] = gcs_client.get_blob_content()

    # 5. Process and Move
    if blob_data is not None:
        log.info(f"Successfully read {len(blob_data)} bytes of data.") 
        
        # --- Data Processing Step ---
        raw_data: List[str] = [line for line in blob_data.split('\n') if line.strip() != '']
        log.info(f"Raw Data Sample (first 5 lines): {raw_data[:5]}")

        # Target path format: processed_data/replies.txt
        target_blob_name: str = f"{TARGET_FOLDER}/{blob_path}"
        
        # 6. Move the blob (Copy + Delete)
        move_success: bool = gcs_client.move_blob(
            target_bucket_name=bucket_name,        
            target_blob_name=target_blob_name,     
        )
        
        if not move_success:
            log.error(f"Failed to move blob to {target_blob_name}. Check GCS logs for details.")

    else:
        log.error("Failed to retrieve blob content. Stopping processing.")

    log.info("Finished handling GCS event.")