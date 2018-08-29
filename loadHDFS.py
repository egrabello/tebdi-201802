import json
import os
import sys
from pyspark.sql import SparkSession

# Aborts execution if no command line argument is provided
if len(sys.argv) < 2:
    print('Invalid argument! Please inform a valid UTC TimeStamp.')
    sys.exit(1)

# Retrieves input parameters from config file
configs = json.loads(open("config.json").read())
baseURL = configs['baseURL']
baseBlob = configs['baseBlob']
baseHDFS = configs['baseHDFS']

# Define sys.argv[1] as input date
inputDate = sys.argv[1]

# Data source
blobPath = baseURL + baseBlob + "/y={0}/m={1}/d={2}/h={3}/m=00/PT1H.json".format(inputDate[0:4],inputDate[5:7],inputDate[8:10],inputDate[11:13])

# Opens JSON file and load content to a "dict" variable
inputFile = json.loads(open(blobPath).read())

# Removes unnecessary attributes
for item in inputFile['records']:
    item.pop('Level', None)
    item.pop('category', None)
    item.pop('operationName', None)
    item.pop('resourceId', None)
    item.update(item['properties'])
    item.pop('properties', None)
    item.pop('isRequestSuccess', None)
    item.pop('clientTime', None)

# Writes temporary JSON file with Spark SQL compatible format
tempOutput = open(baseURL + "/temp.json", "w")
json.dump(inputFile['records'], tempOutput)
tempOutput.close()

# Creates SparkSession
spark = SparkSession.builder.appName("Spark SQL - HDFS Ingestion").getOrCreate()

# Loads temporary JSON to DataFrame and writes to HDFS, grouping by YYYY-MM-DD
df = spark.read.json(baseURL + "/temp.json")
outputPath = baseHDFS + "/{0}-{1}-{2}.json".format(inputDate[0:4],inputDate[5:7],inputDate[8:10])
df.write.save(outputPath, format='json', mode='append')

# Removes temporary JSON file
if os.path.exists(baseURL + "/temp.json"):
    os.remove(baseURL + "/temp.json")
