# prelim manual work


## insturctions
[email](https://mail.google.com/mail/u/2/#inbox/FMfcgzQfCDKwSJjnjksVDNtGHbxTKXPC)

Apologies for the delays to this delivery, I can confirm that the data is now available for download from Fathom's AWS S3 service. Please follow the instructions below to access the layers.

Step 1
Ensure that the AWS CLI tools are installed on your system. You can find detailed installation instructions for various operating systems here: AWS CLI Installation Guide.

Step 2
Open the attached script in any text editor and replace the placeholders with your Access Key (username) and Secret Access Key (credential) available here. Please note, for security reasons, this link will expire after 7 days.

Step 3 (OS dependent)
Windows: Save the script with a .bat file extension in the folder to which you want to download Fathom's data.

Linux / Mac OS:
Add the line '#!/bin/bash' (without the quotes) at the start of the script, then save with a .sh file extension in the folder to which you want to download Fathom's data

Make the script executable by running 'chmod +x script_file_name.sh' via the command line, replacing 'script_file_name' with the actual name of your script

Step 4
Execute the script to download your data.
Please let me know if you have any issues with accessing the data.
 



## definitive no-access check (WSL)
```bash
# show exactly which IAM identity is being used
aws sts get-caller-identity --query Arn --output text

# definitive proof command: should fail with AccessDenied on s3:GetObject
aws s3api get-object \
  --bucket fathom-products-flood \
  --key flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in10-COASTAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1/n00e006.tif \
  /workspace/n00e006.tif
echo "exit_code=$?"
```

Expected result: `AccessDenied` and non-zero exit code. 

WARNING: you probably only have access to certain assets. 

 



# S3 download script (WSL)
 
## Install AWS CLI (WSL)

```bash
sudo apt update
sudo apt install -y awscli
aws --version

export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_KEY"
export AWS_DEFAULT_REGION="eu-west-2"
# If using temporary credentials:
# export AWS_SESSION_TOKEN="YOUR_SESSION_TOKEN"
out_dir=/home/cefect/LS/10_IO/2407_FHIMP/fathom

./fathom_fetch.sh $out_dir

```
## check
```bash
# get TSV of file sizes in GB for each directory in out-dir
{
  echo -e "filename\tfilesize_gb\tfile_count"
  find "$out_dir" -mindepth 1 -maxdepth 1 -type d -print0 \
    | while IFS= read -r -d '' d; do
        size_b=$(du -s -B1 "$d" | awk '{print $1}')
        file_count=$(find "$d" -type f | wc -l)
        name=$(basename "$d")
        printf "%s\t%.3f\t%s\n" "$name" "$(awk "BEGIN {print $size_b/1024/1024/1024}")" "$file_count"
      done \
    | sort -t$'\t' -k2,2nr
} > fetch_size.tsv
```
