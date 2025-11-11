#!/usr/bin/env python3
import sys
import os
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from azure.core.exceptions import ResourceExistsError
from datetime import datetime, timedelta

def upload_file_to_blob_sas(file_path, container_name, blob_name):
    """Upload a file to Azure Blob Storage using SAS token"""
    
    account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
    sas_token = os.getenv('AZURE_STORAGE_SAS_TOKEN')
    
    if not account_name or not sas_token:
        print("Error: AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_SAS_TOKEN environment variables not set")
        return False
    
    try:
        # Create BlobServiceClient using SAS token
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
        
        # Get container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Create container if it doesn't exist
        try:
            container_client.create_container()
            print(f"Container '{container_name}' created successfully")
        except ResourceExistsError:
            print(f"Container '{container_name}' already exists")
        
        # Upload file
        with open(file_path, "rb") as data:
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(data, overwrite=True)
        
        print(f"Successfully uploaded {file_path} to {blob_name}")
        return True
        
    except Exception as e:
        print(f"Error uploading to Azure Blob Storage: {str(e)}")
        return False

def list_containers_sas():
    """List containers using SAS token (for testing)"""
    account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
    sas_token = os.getenv('AZURE_STORAGE_SAS_TOKEN')
    
    if not account_name or not sas_token:
        print("Error: AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_SAS_TOKEN environment variables not set")
        return False
    
    try:
        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
        
        containers = blob_service_client.list_containers()
        print("Available containers:")
        for container in containers:
            print(f" - {container.name}")
        return True
    except Exception as e:
        print(f"Error listing containers: {str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 azure_upload.py <file_path> <container_name> <blob_name>")
        print("Environment variables required:")
        print("  - AZURE_STORAGE_ACCOUNT_NAME")
        print("  - AZURE_STORAGE_SAS_TOKEN")
        sys.exit(1)
    
    file_path = sys.argv[1]
    container_name = sys.argv[2]
    blob_name = sys.argv[3]
    
    if not os.path.exists(file_path):
        print(f"Error: File does not exist: {file_path}")
        sys.exit(1)
    
    success = upload_file_to_blob_sas(file_path, container_name, blob_name)
    sys.exit(0 if success else 1)