#!/usr/bin/env python3
"""
Test script to verify AWS S3 connection and credentials
"""

import json
import boto3
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError

def test_aws_connection():
    """Test AWS credentials and S3 bucket access"""
    
    # Load config
    config_file = Path("aws_config.json")
    if not config_file.exists():
        print("Error: aws_config.json not found")
        return
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    aws_config = config.get('aws', {})
    access_key = aws_config.get('access_key_id')
    secret_key = aws_config.get('secret_access_key')
    region = aws_config.get('region', 'us-east-1')
    bucket_name = aws_config.get('bucket_name')
    
    print("=" * 60)
    print("AWS Connection Test")
    print("=" * 60)
    print(f"Region: {region}")
    print(f"Bucket: {bucket_name}")
    print(f"Access Key ID: {access_key[:10]}..." if access_key else "Not set")
    print("=" * 60)
    
    # Test 1: Verify credentials with STS
    print("\n1. Testing AWS credentials...")
    try:
        sts_client = boto3.client(
            'sts',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        identity = sts_client.get_caller_identity()
        print(f"   ✓ Credentials valid!")
        print(f"   Account ID: {identity.get('Account')}")
        print(f"   User ARN: {identity.get('Arn')}")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"   ✗ Credential error: {error_code}")
        print(f"   Message: {e.response.get('Error', {}).get('Message', str(e))}")
        return
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return
    
    # Test 2: Check S3 access
    print("\n2. Testing S3 access...")
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        
        # Try to head the bucket (check if it exists and we have access)
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"   ✓ Bucket '{bucket_name}' exists and is accessible!")
        
        # List some objects (first 5)
        print(f"\n3. Listing objects in bucket...")
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=5)
        if 'Contents' in response:
            print(f"   Found {response.get('KeyCount', 0)} objects (showing first 5):")
            for obj in response.get('Contents', [])[:5]:
                print(f"   - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("   Bucket is empty")
            
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        print(f"   ✗ S3 Error: {error_code}")
        print(f"   Message: {error_msg}")
        
        if error_code == '404':
            print(f"\n   The bucket '{bucket_name}' does not exist.")
            print(f"   Please verify the bucket name is correct.")
        elif error_code == '403':
            print(f"\n   Access denied to bucket '{bucket_name}'.")
            print(f"   Check your IAM permissions for this bucket.")
        elif error_code == 'InvalidAccessKeyId':
            print(f"\n   Invalid Access Key ID.")
        elif error_code == 'SignatureDoesNotMatch':
            print(f"\n   Invalid Secret Access Key.")
    except Exception as e:
        print(f"   ✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    test_aws_connection()

