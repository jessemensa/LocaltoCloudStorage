import apache_beam as beam 
import os # access to operating system, pass variables, access information 



serviceAccount = "/Users/jessmensa/Desktop/batchdataflow/dataflow-1-370714-39edceda5acd.json" # key 
# when writing file to gcp storage, will be hitting the API
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount # environment variable

p1 = beam.Pipeline() 

# function recieves a beam do function 
# when using a beam function, every class that function is created must return a single record 
class Filter(beam.DoFn):
    # function process and parameters 
    def process(self, record):
        if int(record[8]) > 0:
            return [record] 

Delayed_time = (
    p1 
    | "Import Data time" >> beam.io.ReadFromText("voos_sample.csv", skip_header_lines=1) # readfromtext method to read the csv file 
    | "Split by comma time" >> beam.Map(lambda record: record.split(',')) 
    | "Filter Delays time" >> beam.ParDo(Filter()) 
    | "Create key value-value time" >> beam.Map(lambda record: (record[4], int(record[8]))) 
    | "sum by key time" >> beam.CombinePerKey(sum) 
)


Delayed_num = (
    p1 
    | "Import Data" >> beam.io.ReadFromText("voos_sample.csv", skip_header_lines=1) # readfromtext method to read the csv file 
    | "Split by comma" >> beam.Map(lambda record: record.split(',')) 
    | "Filter Delays" >> beam.ParDo(Filter())
    | "Create key value pair" >> beam.Map(lambda record: (record[4], int(record[8]))) 
    | "combine by key" >> beam.combiners.Count.PerKey() 
)


Delay_table = (
    {'Delayed_num': Delayed_num, 'Delayed_time': Delayed_time}
    | beam.CoGroupByKey() 
    | beam.io.WriteToText("gs://dataflow-one-je/final_output.csv") # WRITE TO THE BUCKET
)

p1.run() 

# WHEN IT RUN, ITS GOING TO BE HITTING THE STORAGE BUCKET API AND ASK FOR GOOGLE APPLICATION CREDENTIALS 
# THEN WE HAND OVER THE CREDENTIALS AND IT WORKS 