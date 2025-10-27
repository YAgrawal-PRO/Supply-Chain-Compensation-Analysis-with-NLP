# main.py

import functions_framework as ff
import logging as log
from typing import Dict, Any
from cloudevents.http import CloudEvent

# Local import of the core handler function
from modules.handler import handle_gcs_event_data

# Configure basic logging (if not done globally elsewhere)
log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s: %(message)s')


@ff.cloud_event
def data_ingest_process(cloud_event: CloudEvent) -> None:
    """
    The entry point for the Cloud Function, triggered by a GCS event.

    Args:
        cloud_event (CloudEvent): The Cloud Event object containing GCS event data.
    """
    log.info("--- Cloud Function Invoked ---")
    
    # 1. Access the GCS event data payload
    data: Dict[str, Any] = cloud_event.data
    
    # 2. Pass the data to the dedicated handler for processing
    handle_gcs_event_data(data)
    
    log.info("--- Cloud Function Execution Complete ---")