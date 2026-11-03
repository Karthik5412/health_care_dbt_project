import dlt

@dlt.table(name= 'full_df') 
def full_df() :
    df = spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format','csv')\
        .option('cloudFiles.inferColumnTypes',True)\
        .option('header',True)\
        .load('/Volumes/health_care/source/data/')

    return df 


@dlt.table(name= 'patient') 
def patient():
    df = dlt.read_stream('full_df')

    return df.select('patient_id','age_range','gender','ethnicity','region')


@dlt.table(name= 'condition') 
def condition():
    df = dlt.read_stream('full_df')

    return df.select('patient_id','icd10_code','condition','severity','onset_year')



@dlt.table(name= 'medication') 
def medication():
    df = dlt.read_stream('full_df')

    return df.select('patient_id','icd10_code','medication')



@dlt.table(name= 'health_data') 
def health_data():
    df = dlt.read_stream('full_df')

    return df.select('patient_id','icd10_code','num_visits','adherence_pct','systolic_bp','diastolic_bp','heart_rate','cholesterol_mg_dl','bmi','outcome')





