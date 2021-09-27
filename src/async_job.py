from Table import Table
import pandas as pd
import queries
from sqlalchemy import create_engine
import pyodbc
import urllib
import json
from datetime import datetime

# SQL Server credentials
server = 'aftermarket-mhirj.database.windows.net'
database = 'reldata'
username = 'reldata_ro'
password = '{Pending-Savings5-Brunch-Yearbook}'

driver = '{SQL Server}'
conn = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';TrustServerCertificate=no;UID=' + username + ';PWD=' + password
params = urllib.parse.quote_plus(conn)
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine_azure = create_engine(conn_str)


# This function is called regularly and is used to obtain heatmaps of past
# 12 months from the Jan of current year
def async_job(array_of_all_metrics, k):
    # Initiate the dictionary that will be dumped into json
    static_json_builder = dict()
    currentYear = datetime.now().year
    date_lower_boundary = str(currentYear - 1) + '-01-01'
    date_upper_boundary = str(currentYear) + '-01-01'

    for metric in array_of_all_metrics:
        # Instantiate instance for each metric
        theMetric = Table(engine_azure)

        # get the query from queries module
        query = getattr(queries, metric)
        query = query.format(date_lower_boundary, date_upper_boundary, "('CRJ700')")

        theMetric.query_table(query)
        theMetric.select_top_K_number_for_each_operator(k)

        static_json_builder[metric] = theMetric.generate_heatmap_html()
        static_json_builder[metric + '_colorBar'] = theMetric.generate_heatmap_colorBar_image_encode()

    # dump the static json builder to json file
    with open('./static/html.json', 'w') as data:
        json.dump(static_json_builder, data)
    print('success!')


def async_get_regions():
    region_query = queries.operators_regions
    region_df = pd.read_sql(region_query, engine_azure, index_col='OPERATOR_NAME')
    region_df.duplicated()

    regions_dic = dict()
    for index in region_df.index:
        regions_dic[index] = region_df.loc[index, 'REGION']

    with open('./static/region.json', 'w') as region:
        json.dump(regions_dic, region)
    print('region_update_success!')


if __name__ == "__main__":
    async_get_regions()
    currentYear = datetime.now().year
    date_lower_boundary = str(currentYear - 1) + '-01-01'
    date_upper_boundary = str(currentYear) + '-01-01'
    query = getattr(queries, 'DIR')
    query = query.format(date_lower_boundary, date_upper_boundary)
    table = Table(engine_azure)
    df = table.query_table(query)
    table.select_top_K_number_for_each_operator(20)
    print(table.value_table)
