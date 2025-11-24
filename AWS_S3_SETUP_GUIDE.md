# AWS S3 Bucket and IAM Policy Setup Guide

This guide will walk you through creating an S3 bucket and setting up IAM permissions for the weather data application.

## Prerequisites

- AWS Account with access to IAM and S3 services
- IAM User: `karthik-aws` (already exists)
- AWS Access Key ID and Secret Access Key (already configured in `aws_config.json`)

## Step 1: Create S3 Bucket in AWS Console

1. **Navigate to S3 Service**
   - Go to [AWS Console](https://console.aws.amazon.com/)
   - Search for "S3" in the services search bar
   - Click on "S3" service

2. **Create New Bucket**
   - Click the orange "Create bucket" button
   - You'll see a multi-step configuration wizard

3. **Configure General Settings**
   - **Bucket name**: 
     - Choose a globally unique name (e.g., `karthik-weatherdata` or `my-weather-bucket-2024`)
     - Must be lowercase, no spaces, can use hyphens
     - Write down the exact name - you'll need it later!
   - **AWS Region**: 
     - Select `us-east-1` (N. Virginia) or your preferred region
     - Note: This should match the region in your `aws_config.json`

4. **Configure Object Ownership**
   - Select "ACLs disabled (recommended)" or "Bucket owner preferred"
   - This controls who owns objects uploaded to the bucket

5. **Configure Block Public Access Settings**
   - **Keep all 4 settings enabled** (recommended for security)
   - This ensures your bucket is private and not publicly accessible
   - You can change this later if needed

6. **Configure Bucket Versioning**
   - Select "Disable" (unless you need version history)
   - Versioning keeps multiple versions of files but costs more

7. **Configure Default Encryption**
   - Select "Enable"
   - Choose "Server-side encryption with Amazon S3 managed keys (SSE-S3)"
   - This encrypts your data at rest

8. **Advanced Settings**
   - Leave Object Lock disabled (unless you need it)
   - Leave Event Notifications disabled (unless you need them)

9. **Review and Create**
   - Review all settings
   - Click "Create bucket" at the bottom
   - Wait for confirmation that the bucket was created

## Step 2: Create IAM Policy for S3 Access

1. **Navigate to IAM Service**
   - Go to AWS Console
   - Search for "IAM" in the services search bar
   - Click on "IAM" service

2. **Find Your User**
   - Click "Users" in the left sidebar
   - Search for or click on `karthik-aws`
   - You should see the user details page

3. **Add Inline Policy**
   - Click the "Add permissions" dropdown button
   - Select "Create inline policy"
   - This opens the policy editor

4. **Configure Policy**
   - Click the "JSON" tab (not the Visual editor)
   - Delete any existing content in the editor
   - Copy and paste the following policy:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "AllowS3WeatherDataAccess",
         "Effect": "Allow",
         "Action": [
           "s3:PutObject",
           "s3:GetObject",
           "s3:ListBucket"
         ],
         "Resource": [
           "arn:aws:s3:::YOUR_BUCKET_NAME",
           "arn:aws:s3:::YOUR_BUCKET_NAME/*"
         ]
       }
     ]
   }
   ```

5. **Replace Bucket Name**
   - Find `YOUR_BUCKET_NAME` in the policy (appears twice)
   - Replace it with your actual bucket name from Step 1
   - Example: If your bucket is `karthik-weatherdata`, the Resources should be:
     - `arn:aws:s3:::karthik-weatherdata`
     - `arn:aws:s3:::karthik-weatherdata/*`

6. **Review and Create Policy**
   - Click "Next" button
   - Enter a policy name: `S3WeatherDataAccess`
   - (Optional) Add description: "Allows access to weather data S3 bucket"
   - Click "Create policy"
   - You should see a success message

7. **Verify Policy**
   - On the user details page, scroll to "Permissions" section
   - You should see "S3WeatherDataAccess" listed under "Inline policies"

## Step 3: Update Local Configuration

After creating the bucket, update your `aws_config.json` file:

1. **Open** `aws_config.json` in your project
2. **Update** the `bucket_name` field with your new bucket name
3. **Optionally update** `s3_prefix` if you want a different folder structure
4. **Keep** all other fields unchanged (credentials, region)

Example:
```json
{
  "aws": {
    "access_key_id": "YOUR_ACCESS_KEY_ID",
    "secret_access_key": "YOUR_SECRET_ACCESS_KEY",
    "region": "us-east-1",
    "bucket_name": "your-new-bucket-name",
    "s3_prefix": "weather-data"
  }
}
```

**Note:** Replace `YOUR_ACCESS_KEY_ID` and `YOUR_SECRET_ACCESS_KEY` with your actual AWS credentials from your IAM user.

## Step 4: Verify Setup

Run the test script to verify everything is working:

```bash
python3 test_aws_connection.py
```

You should see:
- ✓ Credentials valid
- ✓ Bucket exists and is accessible
- (Optional) List of objects in the bucket

If you see any errors, check:
1. Bucket name matches exactly (case-sensitive)
2. Region matches in both AWS Console and config file
3. IAM policy was created and attached to the user
4. Policy Resources use the correct bucket name

## Step 5: Test Weather Data Upload

Once verified, test the full workflow:

```bash
python3 weather_to_json.py
```

Enter your OpenWeather API key and a city name. The script should successfully upload the JSON file to S3.

## Troubleshooting

### Error: AccessDenied
- Verify IAM policy is attached to the correct user
- Check that bucket name in policy matches exactly
- Ensure policy Resources include both bucket and bucket/*

### Error: NoSuchBucket
- Verify bucket name is spelled correctly
- Check that region matches between bucket and config
- Ensure bucket exists in AWS Console

### Error: InvalidAccessKeyId
- Verify credentials in `aws_config.json` are correct
- Check that IAM user still exists and is active

## Security Best Practices

1. **Never commit** `aws_config.json` to git (already in `.gitignore`)
2. **Rotate credentials** periodically
3. **Use least privilege** - only grant necessary S3 permissions
4. **Enable encryption** on the bucket (done in Step 1)
5. **Keep bucket private** - Block Public Access enabled

## Next Steps

After setup is complete:
- Your weather data will be stored in S3 at: `s3://your-bucket-name/weather-data/`
- Files will be named: `CityName_YYYYMMDD_HHMMSS.json`
- You can view files in AWS Console → S3 → Your Bucket

