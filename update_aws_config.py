#!/usr/bin/env python3
"""
Helper script to update aws_config.json with new bucket name after S3 bucket creation
"""

import json
import sys
from pathlib import Path


def update_bucket_config(bucket_name, s3_prefix=None, region=None):
    """
    Update aws_config.json with new bucket name and optional settings
    
    Args:
        bucket_name (str): New S3 bucket name
        s3_prefix (str, optional): S3 prefix/folder path
        region (str, optional): AWS region
    """
    config_file = Path("aws_config.json")
    
    if not config_file.exists():
        print("Error: aws_config.json not found!")
        print("Please create the file first with your AWS credentials.")
        return False
    
    try:
        # Read existing config
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        if 'aws' not in config:
            print("Error: 'aws' key not found in config file!")
            return False
        
        # Store old values for display
        old_bucket = config['aws'].get('bucket_name', 'not set')
        old_prefix = config['aws'].get('s3_prefix', '')
        old_region = config['aws'].get('region', 'us-east-1')
        
        # Update bucket name
        config['aws']['bucket_name'] = bucket_name
        print(f"✓ Updated bucket_name: {old_bucket} → {bucket_name}")
        
        # Update s3_prefix if provided
        if s3_prefix is not None:
            config['aws']['s3_prefix'] = s3_prefix
            print(f"✓ Updated s3_prefix: {old_prefix} → {s3_prefix}")
        else:
            print(f"  Keeping s3_prefix: {config['aws'].get('s3_prefix', '')}")
        
        # Update region if provided
        if region is not None:
            config['aws']['region'] = region
            print(f"✓ Updated region: {old_region} → {region}")
        else:
            print(f"  Keeping region: {config['aws'].get('region', 'us-east-1')}")
        
        # Write updated config
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
            f.write('\n')
        
        print(f"\n✓ Successfully updated aws_config.json")
        print(f"\nNext steps:")
        print(f"1. Verify the bucket exists in AWS Console")
        print(f"2. Ensure IAM policy is attached to user 'karthik-aws'")
        print(f"3. Run: python3 test_aws_connection.py")
        
        return True
        
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config file: {e}")
        return False
    except Exception as e:
        print(f"Error updating config: {e}")
        return False


def main():
    """Interactive script to update AWS config"""
    print("=" * 60)
    print("AWS Config Updater")
    print("=" * 60)
    print("\nThis script will update aws_config.json with your new S3 bucket name.")
    print("Make sure you've already created the bucket in AWS Console.\n")
    
    # Get bucket name
    bucket_name = input("Enter your new S3 bucket name: ").strip()
    if not bucket_name:
        print("Error: Bucket name cannot be empty!")
        sys.exit(1)
    
    # Get optional s3_prefix
    print("\nS3 Prefix (folder path) - optional")
    print("Example: 'weather-data' or 'weather-data/2024'")
    print("Press Enter to keep current value or leave empty")
    s3_prefix_input = input("Enter S3 prefix: ").strip()
    s3_prefix = s3_prefix_input if s3_prefix_input else None
    
    # Get optional region
    print("\nAWS Region - optional")
    print("Press Enter to keep current value (us-east-1)")
    region_input = input("Enter AWS region: ").strip()
    region = region_input if region_input else None
    
    print("\n" + "=" * 60)
    
    # Update config
    success = update_bucket_config(bucket_name, s3_prefix, region)
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()

