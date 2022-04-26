from async_job import async_job, async_get_regions, engine_azure
from Table import Table
import queries
from flask import Flask, request, make_response
from apscheduler.schedulers.background import BackgroundScheduler
import json
import datetime
from flask_socketio import SocketIO, emit
import time


# Initiate the Flask
app = Flask(__name__)
app.config['SECRET_KEY'] = 'SECRET'
#CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", ping_timeout=300, logger=True, engineio_logger=True)

number_of_rows_heatmap = 20

scheduler = BackgroundScheduler()
scheduler.add_job(async_job, "interval", days=7, args=[['DIR', 'REMOVAL_RATE'], number_of_rows_heatmap], next_run_time=datetime.datetime.now())
scheduler.add_job(async_get_regions, "interval", days=7, next_run_time=datetime.datetime.now())
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
        aircraft_model = data['ac_model'].split(',')
        # adding placeholder in models list
        for _ in range(len(aircraft_model), 3):
            aircraft_model.append('')

        upper_year_boundary = str(data['year'])
        upper_month_boundary = str(data['month'])
        if len(upper_month_boundary) == 1:
            upper_month_boundary = '0' + upper_month_boundary

        lower_year_boundary = str(int(upper_year_boundary)-1)
        date_upper_boundary = upper_year_boundary + '-' + upper_month_boundary + '-01'
        date_lower_boundary = lower_year_boundary + '-' + upper_month_boundary + '-01'

        # getting the query
        query = getattr(queries, metric)
        query = query.format(date_lower_boundary, date_upper_boundary, aircraft_model[0], aircraft_model[1], aircraft_model[2])

        # create Table instance to query data and generate heatmap
        table = Table(engine_azure)
        table.query_table(query)
        table.select_top_K_number_for_each_operator(number_of_rows_heatmap)

        heatmap_html = table.generate_heatmap_html()
        colorBar_encode = table.generate_heatmap_colorBar_image_encode()

        resp = make_response({'heatmap': heatmap_html, 'colorBar': colorBar_encode})
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp


@socketio.on('initial_request', namespace='/initial')
def heatmap_socket(request):
    print(request)
    with open('./static/html.json') as infile:
        data = json.load(infile)
    emit('initial_request', data)


@socketio.on('subsequent_request', namespace='/subsequent')
def get_heatmap(data):
    print(data)

    # prepare each parameter to be put into sql query
    metric = data['metric']
    aircraft_model = data['ac_model']
    # adding placeholder in models list
    for _ in range(len(aircraft_model), 3):
        aircraft_model.append('')

    upper_year_boundary = str(data['year'])
    upper_month_boundary = str(data['month'])
    if len(upper_month_boundary) == 1:
        upper_month_boundary = '0' + upper_month_boundary

    lower_year_boundary = str(int(upper_year_boundary) - 1)
    date_upper_boundary = upper_year_boundary + '-' + upper_month_boundary + '-01'
    date_lower_boundary = lower_year_boundary + '-' + upper_month_boundary + '-01'

    # getting the query
    query = getattr(queries, metric)
    query = query.format(date_lower_boundary, date_upper_boundary, aircraft_model[0], aircraft_model[1],
                         aircraft_model[2])

    # create Table instance to query data and generate heatmap
    table = Table(engine_azure)
    table.query_table(query)
    table.select_top_K_number_for_each_operator(number_of_rows_heatmap)

    heatmap_html = table.generate_heatmap_html()
    colorBar_encode = table.generate_heatmap_colorBar_image_encode()
    print('sleeping')
    time.sleep(105)

    emit('subsequent_request', {'heatmap': heatmap_html, 'colorBar': colorBar_encode})

if __name__ == "__main__":
    #app.run()
    app.debug=True
    socketio.run(app)