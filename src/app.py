from async_job import async_job, async_get_regions, engine_azure
from Table import Table
import queries
from flask import Flask, request, make_response
from apscheduler.schedulers.background import BackgroundScheduler
from flask_cors import CORS
import json


# Initiate the Flask
app = Flask(__name__)
CORS(app)

number_of_rows_heatmap = 20

scheduler = BackgroundScheduler()
scheduler.add_job(async_job, "interval", seconds=20, args=[['DIR'], number_of_rows_heatmap])
scheduler.add_job(async_get_regions, "interval", seconds=20)
scheduler.start()


@app.route("/", methods=['POST', 'GET'])
def App():
    if request.method == 'GET':

        with open('./static/html.json') as infile:
            data = json.load(infile)

        response = make_response(data)
        response.headers['Access-Control-Allow-Credentials'] = 'true'
        return response

    else:
        # read data from POST request
        data = request.form.to_dict()


        # prepare each parameter to be put into sql query
        metric = data['metric']
        aircraft_model = '(' + str(data['ac_model'].split(','))[1:-1] +')'
        if len(aircraft_model)==4:
            aircraft_model = "('CRJ700')"

        upper_year_boundary = str(data['year'])
        upper_month_boundary = str(data['month'])
        if len(upper_month_boundary) == 1:
            upper_month_boundary = '0' + upper_month_boundary

        lower_year_boundary = str(int(upper_year_boundary)-1)
        date_upper_boundary = upper_year_boundary + '-' + upper_month_boundary + '-01'
        date_lower_boundary = lower_year_boundary + '-' + upper_month_boundary + '-01'

        # getting the query
        query = getattr(queries, metric)
        query = query.format(date_lower_boundary, date_upper_boundary, aircraft_model)

        # create Table instance to query data and generate heatmap
        table = Table(engine_azure)
        table.query_table(query)
        table.select_top_K_number_for_each_operator(number_of_rows_heatmap)

        heatmap_html = table.generate_heatmap_html()
        colorBar_encode = table.generate_heatmap_colorBar_image_encode()

        resp = make_response({'heatmap': heatmap_html, 'colorBar': colorBar_encode})
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp


if __name__ == "__main__":
    app.run()