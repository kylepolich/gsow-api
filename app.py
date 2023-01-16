from chalice import Chalice
from dao import DataAccessObject




app = Chalice(app_name='gsow-api')


@app.route('/health')
def index():
    return {'working': True}


@app.route('/api/v1/data')
def data():
    # TODO: duplicate php api/query
    # TODO: dao.get_something(blah)
    return {'success': False, "error_message": "Not implemented"}
# TODO: 


@app.schedule(Rate(60, unit=Rate.MINUTES))
def periodic_task(event):
    # TODO: pick up where you left off from last time
    pass
