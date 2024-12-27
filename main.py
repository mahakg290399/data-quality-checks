import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace, when, to_date
from pyspark.sql.types import *
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def validate_data_quality(glueContext, input_path, output_path):
    # Read input CSV file using Glue Dynamic Frame
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"',
            "withHeader": True,
            "separator": ",",
            "optimizePerformance": True
        },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": [input_path],
            "recurse": True
        }
    )
    
    # Convert to DataFrame for easier processing
    df = dynamic_frame.toDF()
    
    # Initialize validation results list
    validation_results = []
    
    # Define validation rules and apply them
    
    # 1. Required Field Validations
    required_fields = {
        'DocumentCount': 'integer',
        'AmendedReturn': 'boolean',
        'FAMLIPremiumStartDate': 'date',
        'FAMLIPremiumEndDate': 'date',
        'SettlementDate': 'datetime',
        'EmployerLegalName': 'string',
        'BusAdrStreet1': 'string',
        'BusAdrCity': 'string',
        'BusAdrStateCode': 'string',
        'BusAdrPostalCode': 'string',
        'BusAdrCountry': 'string',
        'TotalWagesThisPeriod': 'numeric',
        'PaymentAmountTotal': 'numeric',
        'IsFinalReturn': 'boolean',
        'EmployeeSSN': 'string',
        'EmployeeFirstName': 'string',
        'EmployeeLastName': 'string',
        'YearToDateWages': 'numeric',
        'GrossWagesThisQtr': 'numeric',
        'SubjectWagesThisQtr': 'numeric',
        'FAMLIContributionThisQtr': 'numeric'
    }
    
    for field, data_type in required_fields.items():
        # Check for null values in required fields
        null_count = df.filter(col(field).isNull()).count()
        if null_count > 0:
            validation_results.append({
                'field': field,
                'issue': f'Missing required field: {field}',
                'count': null_count
            })
        
        # Data type validations
        if data_type == 'date':
            date_format_issues = df.filter(
                col(field).isNotNull() & 
                ~col(field).rlike('^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
            ).count()
            if date_format_issues > 0:
                validation_results.append({
                    'field': field,
                    'issue': f'Invalid date format in {field}',
                    'count': date_format_issues
                })
                
        elif data_type == 'numeric':
            numeric_issues = df.filter(
                col(field).isNotNull() & 
                ~col(field).cast('double').isNotNull()
            ).count()
            if numeric_issues > 0:
                validation_results.append({
                    'field': field,
                    'issue': f'Invalid numeric value in {field}',
                    'count': numeric_issues
                })
    
    # 2. Specific Field Validations
    
    # SSN Format validation
    ssn_format_issues = df.filter(
        col('EmployeeSSN').isNotNull() & 
        ~col('EmployeeSSN').rlike('^[0-9]{9}$')
    ).count()
    if ssn_format_issues > 0:
        validation_results.append({
            'field': 'EmployeeSSN',
            'issue': 'Invalid SSN format',
            'count': ssn_format_issues
        })
    
    # State Code validation
    state_code_issues = df.filter(
        col('BusAdrStateCode').isNotNull() & 
        ~col('BusAdrStateCode').rlike('^[A-Z]{2}$')
    ).count()
    if state_code_issues > 0:
        validation_results.append({
            'field': 'BusAdrStateCode',
            'issue': 'Invalid state code format',
            'count': state_code_issues
        })
    
    # Postal Code validation
    postal_code_issues = df.filter(
        col('BusAdrPostalCode').isNotNull() & 
        ~col('BusAdrPostalCode').rlike('^[0-9]{5}(?:-[0-9]{4})?$')
    ).count()
    if postal_code_issues > 0:
        validation_results.append({
            'field': 'BusAdrPostalCode',
            'issue': 'Invalid postal code format',
            'count': postal_code_issues
        })
    
    # FEIN validation (when required)
    fein_issues = df.filter(
        (col('EmployerFEIN').isNotNull()) & 
        ~col('EmployerFEIN').rlike('^[0-9]{9}$')
    ).count()
    if fein_issues > 0:
        validation_results.append({
            'field': 'EmployerFEIN',
            'issue': 'Invalid FEIN format',
            'count': fein_issues
        })
    
    # 3. Business Logic Validations
    
    # Validate premium dates are in correct order
    date_order_issues = df.filter(
        to_date(col('FAMLIPremiumEndDate')) < to_date(col('FAMLIPremiumStartDate'))
    ).count()
    if date_order_issues > 0:
        validation_results.append({
            'field': 'FAMLIPremiumDates',
            'issue': 'End date is before start date',
            'count': date_order_issues
        })
    
    # Validate contribution calculations
    contribution_issues = df.filter(
        (col('SubjectWagesThisQtr') > col('GrossWagesThisQtr')) |
        (col('YearToDateWages') < col('GrossWagesThisQtr'))
    ).count()
    if contribution_issues > 0:
        validation_results.append({
            'field': 'WagesCalculation',
            'issue': 'Invalid wage calculations',
            'count': contribution_issues
        })
    
    # Create validation results dataframe
    validation_df = spark.createDataFrame(validation_results)
    
    # Convert back to DynamicFrame
    validation_dynamic_frame = DynamicFrame.fromDF(validation_df, glueContext, "validation_results")
    
    # Write validation results to output location
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path_with_timestamp = f"{output_path}/validation_results_{timestamp}"
    
    glueContext.write_dynamic_frame.from_options(
        frame=validation_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": output_path_with_timestamp
        },
        format="csv"
    )

# Main execution
input_path = args['input_path']
output_path = args['output_path']

validate_data_quality(glueContext, input_path, output_path)
job.commit()