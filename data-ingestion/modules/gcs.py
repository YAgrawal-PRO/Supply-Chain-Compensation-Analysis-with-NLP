# modules/gcs.py

from google.cloud import storage
from google.cloud.storage.client import Client
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket
import logging as log
from typing import Optional, Tuple, Union

# NOTE: Pylance warnings about missing stubs for google-cloud-storage are expected and harmless.

class GCSClient:
    """
    Class to perform reusable operations on Google Cloud Storage (GCS) buckets and blobs.
    It initializes the storage client for re-use across method calls.
    """
    # Type hints for instance variables
    storage_client: Client
    bucket_name: Optional[str]
    blob_name: Optional[str]

    def __init__(self, bucket_name: Optional[str] = None, blob_name: Optional[str] = None) -> None:
        """
        Initializes the GCS client with optional default bucket/blob names.
        """
        # 1. Initialize the GCS client once
        self.storage_client = storage.Client()
        log.debug("GCS storage client initialized.")

        # 2. Assign optional default names
        self.bucket_name = bucket_name
        self.blob_name = blob_name

    def _get_validated_names(self, bucket_name: Optional[str], blob_name: Optional[str], context: str) -> Optional[Tuple[str, str]]:
        """
        Internal helper to prioritize argument names over default names and validate they are present.
        """
        # 1. Determine the names to use (argument overrides default)
        target_bucket_name: Optional[str] = bucket_name or self.bucket_name
        target_blob_name: Optional[str] = blob_name or self.blob_name

        log.debug(f"[{context}] Validating names. Result: gs://{target_bucket_name}/{target_blob_name}")

        # 2. Validate that both names are available
        if target_bucket_name is None or target_blob_name is None:
            log.error(f"Error ({context}): Both bucket_name and blob_name must be provided.")
            return None
        
        # 3. Return as a tuple of non-None strings
        return target_bucket_name, target_blob_name

    def _get_blob_or_none(self, bucket_name: str, blob_name: str) -> Optional[Blob]:
        """Internal helper to get the Blob object, handling exceptions."""
        try:
            # 1. Get the bucket and blob references
            bucket: Bucket = self.storage_client.bucket(bucket_name)
            blob: Blob = bucket.blob(blob_name)
            return blob
        except Exception as e:
            log.error(f"GCSClient: Error getting blob reference gs://{bucket_name}/{blob_name}: {e}")
            return None

    # --- Core GCS Access Methods ---

    def get_blob(self, bucket_name: Optional[str] = None, blob_name: Optional[str] = None) -> Optional[Blob]:
        """
        Retrieves a blob object from the specified GCS bucket.
        """
        # 1. Validate and retrieve required names
        names = self._get_validated_names(bucket_name, blob_name, "get_blob")
        if names is None:
            return None
        
        target_bucket_name, target_blob_name = names
        
        # 2. Get the Blob object reference
        blob = self._get_blob_or_none(target_bucket_name, target_blob_name)
        
        if blob:
            log.info(f"GCSClient: Reference created for blob: gs://{target_bucket_name}/{target_blob_name}")
        
        return blob


    def get_blob_content(self, bucket_name: Optional[str] = None, blob_name: Optional[str] = None, encoding: str = "utf-8") -> Union[str, bytes, None]:
        """
        Fetches the text content of a blob from a GCS bucket.
        """
        # 1. Get the blob object
        blob: Optional[Blob] = self.get_blob(bucket_name, blob_name)

        if blob is None:
            log.warning("GCSClient: Skipping content read as blob reference is missing.")
            return None

        log.debug(f"GCSClient: Reading content from {blob.name} with encoding '{encoding}'.")
        try:
            # 2. Open blob in read text mode ('r')
            with blob.open("r", encoding=encoding) as f:
                content: Union[str, bytes] = f.read()
            
            log.info("GCSClient: Successfully read blob content.")
            return content
        except Exception as e:
            # 3. Handle read errors
            log.error(f"GCSClient: Failed to read content for blob '{blob.name}': {e}")
            return None

    # --- Data Management Method ---

    def move_blob(self, source_bucket_name: Optional[str] = None, source_blob_name: Optional[str] = None, target_bucket_name: Optional[str] = None, target_blob_name: Optional[str] = None) -> bool:
        """
        Moves a blob from one GCS location to another (Copy + Delete).
        
        Returns:
            bool: True if move was successful, False otherwise.
        """
        # 1. Validate Source names
        source_names = self._get_validated_names(source_bucket_name, source_blob_name, "move_blob source")
        if source_names is None: return False
        _source_bucket_name, _source_blob_name = source_names

        # 2. Determine Target names: Target bucket defaults to source bucket; target blob defaults to source blob
        _target_bucket_name: str = target_bucket_name or self.bucket_name or _source_bucket_name
        _target_blob_name: str = target_blob_name or _source_blob_name
        
        # 3. Safety Check: Same location?
        if _source_blob_name == _target_blob_name and _source_bucket_name == _target_bucket_name:
            log.warning(f"GCSClient: Source and target locations are the same. No action taken.")
            return False

        log.info(f"GCSClient: Moving gs://{_source_bucket_name}/{_source_blob_name} to gs://{_target_bucket_name}/{_target_blob_name}")
        
        try:
            # 4. Get References
            source_bucket: Bucket = self.storage_client.bucket(_source_bucket_name)
            target_bucket: Bucket = self.storage_client.bucket(_target_bucket_name)
            source_blob: Blob = source_bucket.blob(_source_blob_name)

            # 5. Copy the blob to the target
            source_bucket.copy_blob(source_blob, target_bucket, _target_blob_name)
            log.debug("GCSClient: Copy operation completed.")

            # 6. Delete the original blob
            source_blob.delete()
            log.info("GCSClient: Move successful (Copied and Deleted).")

            return True
        except Exception as e:
            log.error(f"GCSClient: Failed to move blob. Error: {e}")
            return False