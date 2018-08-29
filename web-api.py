import flask
from flask import request, jsonify
from pyspark.sql import SparkSession
import pandas
import json
from sqlFuncs import *

# Input parameters (config file and +)
configs = json.loads(open("config.json").read())
baseHDFS = configs['baseHDFS']
pathLogs = baseHDFS + "/*.json"
pathReports = "reports.json"

# Start Flask
app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Creates SparkSession
spark = SparkSession.builder.appName("Spark SQL - WebAPI").getOrCreate()

# Home page
@app.route('/', methods=['GET'])
def home():
    return '''<h1>Análise de Logs do Barramento de API</h1>
<p>Aluno: Egberto Armando Rabello de Oliveira</p>'''

# Error handling for resources not found
@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>Resource not found!</p>", 404

# Lists all available reports
@app.route('/reports', methods=['GET'])
def reports_all():
    reports = json.loads(open(pathReports).read())
    output = reports
    for item in output:
        output[item].pop('query', None)
    return jsonify(output)

# Run report
@app.route('/reports/<string:reportId>', methods=['GET'])
def reports(reportId):
    # Creates Dataframe and view
    dfLogs = spark.read.json(pathLogs)
    dfLogs.createOrReplaceTempView("requests")
    # Search query corresponding to 'reportId'
    reports = json.loads(open(pathReports).read())
    if reportId in reports:
        query = reports[reportId]['query']
        # Check QueryStrings
        aggregate = request.args.get("aggregate", "")
        days = request.args.get("days", "")
        startDate = request.args.get("startDate", "")
        endDate = request.args.get("endDate", "")
        showQuery = request.args.get("showQuery", "")
        # Applies aggregation and filters
        query = groupByUnit(query, aggregate)
        query = timeFilter(query, days, startDate, endDate)
        # Run query and show results
        queryOutput = spark.sql(query)
        result = {}
        result['result'] = queryOutput.toPandas().to_dict('records')
        if showQuery.lower() == "yes":
            result['query'] = query
        return jsonify(result)
    else:
        return page_not_found(404)

app.run()