This PySpark script implements comprehensive data quality validation based on the requirements shown in the images. Here are the key features:

The script takes input and output S3 paths as arguments
Validates all required fields specified in the images
Performs type checking for all fields
Implements specific format validations for:

SSN (9 digits)
State codes (2 uppercase letters)
Postal codes (5 digits or 5+4 format)
FEIN (9 digits)
Dates (YYYY-MM-DD format)


Validates business logic:

Premium start/end date ordering
Wage calculation consistency
Required field conditional logic



To run the script in AWS Glue:

aws glue start-job-run \
--job-name FAMLI-DQ-glue-job \
--arguments '{
    "--input_path": "s3://your-input-bucket/input-folder/",
    "--output_path": "s3://your-output-bucket/data-quality/"
}'

Here I have assumed that glue job has been created and also have necessary access.

The sript will generate validation results with timestamps in the specified output location. Each validation issue includes:

The field name
Description of the issue
Count of records with that issue

Would you like me to add any additional validations or modify the existing ones?