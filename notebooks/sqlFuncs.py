from datetime import datetime, timedelta

# Function to rewrite SQL by adding [time] per unit (year, month, day, hour, minute, or second)
def groupByUnit(query, unit):
    aggregate = 'time'
    timeMap = {'year': '4', 'month': '7', 'day': '10', 'hour': '13', 'minute': '16', 'second': '19'}
    if (query.lower().find('[time]') >= 0) and (unit.lower() in timeMap):
        mask = '1900-01-01T00:00:00.0Z'
        aggregate = "CONCAT(SUBSTR(time,0," + timeMap[unit.lower()] + "),'" + mask[int(timeMap[unit.lower()]):] + "')"
    return query.replace('[time]', aggregate)   

# Function to add condition to filters in SQL
def addCondition(filters, condition):
    if len(filters) > 0:
        return filters + " AND " + condition
    else:
        return condition

# Function to apply filters to SQL query
def timeFilter(query, days, startDate, endDate):
    # Define date filters
    filters = ''
    if len(days) > 0:
        refDate = str(datetime.now() + timedelta(days=-int(days)))[:10]
        filters = addCondition(filters, "time >= '" + refDate + "'" )
    if len(startDate) > 0:
        filters = addCondition(filters, "time >= '" + startDate + "'" )
    if len(endDate) > 0:
        refDate = str(datetime.strptime(endDate,'%Y-%m-%d') + timedelta(days=1))[:10]
        filters = addCondition(filters, "time < '" + refDate + "'" )

    # Add filters to query
    if len(filters) > 0:
        posWhere = query.lower().find(' where ')
        if posWhere >= 0:
            query = query[0:posWhere] + ' WHERE ' + filters + ' AND ' + query[posWhere+7:]
        else:
            posFrom = query.lower().find(' from requests')
            if posFrom >= 0:
                query = query[0:posFrom] + ' FROM requests WHERE ' + filters + query[posFrom+14:]

    return query
