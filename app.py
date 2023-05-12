from chalice import Chalice
from chalicelib.dao import DataAccessObject
from chalicelib.update import run_update, get_updates_for_page_id
from datetime import datetime
import dateparser
import os


app = Chalice(app_name='gsow-api')

access_key = os.environ.get("ACCESS_KEY")
secret_key = os.environ.get("SECRET_KEY")
region_name = os.environ.get("REGION_NAME")
table_name = os.environ.get("TABLE_NAME")
stream_table = os.environ.get("STREAM_TABLE")

dao = DataAccessObject(access_key, secret_key, region_name, table_name, stream_table)
docstore = dao.get_docstore()
streams = dao.get_streams()


owner = 'wiki-pageviews'


@app.schedule('rate(60 minutes)')
def periodic_task(event):
    run_update()


@app.route('/health')
def index():
    return {'working': True}


@app.route('/pages/{lang}/{pageid}', methods=['POST'])
def pages_add(lang, pageid):
    request_body = app.current_request.json_body
    start = request_body['start'].strip()
    print("start", start, pageid, lang)
    # start = dateparser.parse(start)
    start = datetime.strptime(start, "%Y-%m-%d")
    print("start", start)
    start = str(start)[0:10]
    doc = {
        'owner': owner,
        'pageid': pageid,
        'lang': lang,
        'start': start
    }
    uid = f'{lang}.{pageid}'
    object_id = f'{owner}.{uid}'
    docstore.save_document(object_id, doc)
    items = get_updates_for_page_id(lang, pageid, start)
    print("Updates", len(items))
    if len(items) > 0:
        print(items[0]['stream_id'])
    streams.batch_write_stream(items)
    return { 'success': True, 'object_id': object_id }


@app.route('/pages/{lang}/{pageid}', methods=['GET'])
def pages_get_views(lang, pageid):
    query_params = app.current_request.query_params
    limit = query_params.get('limit', 365)
    stream_id = f'wiki-pageviews.{lang}.{pageid}'
    doc = docstore.get_document(stream_id)
    if doc is None:
        return { 'success': False, 'error': "Not found." }
    items = streams.read_stream_recent(stream_id, limit=int(limit))
    resp = []
    for item in items:
        pv = {
            'dt': datetime.fromtimestamp(item['timestamp']).isoformat(sep='T', timespec='auto'),
            'views': item['views']
        }
        resp.append(pv)
    return { "pageid": pageid, "pageviews": resp }


@app.route('/pages/{lang}/{pageid}', methods=['DELETE'])
def pages_delete(lang, pageid):
    uid = f'{lang}.{pageid}'
    object_id = f'{owner}.{uid}'
    docstore.delete_document(object_id)
    streams.erase_stream(object_id)
    return { 'success': True, 'object_id': object_id }







"""
<?php include("header.php"); ?>

Below are some top viewed pages.

<?php
  $query = "SELECT t1.title, sum(t2.views) as total" .
           ", sum(CASE WHEN t2.dt > NOW() - INTERVAL 3 DAY THEN t2.views ELSE 0 END) as last3 " .
           "FROM contributions t1 " .
           "JOIN page_views t2 " .
           " ON t1.pageid = t2.pageid " .
           "WHERE t2.dt > t1.ts " .
           "GROUP BY t1.title " .
           "ORDER BY sum(CASE WHEN t2.dt > NOW() - INTERVAL 3 DAY THEN t2.views ELSE 0 END) desc " .
           "LIMIT 20";
  $result = mysqli_query($conn, $query);
  echo("<table border=1>");
  echo("<tr><td>Page</td><td>Total Views since first edit</td><td>Total Views last 3 days</td></tr>");
  while ($row = mysqli_fetch_array($result)) {
    echo("<tr>");
    echo("<td>" . $row['title'] . "</td>");
    echo("<td>" . $row['total'] . "</td>");
    echo("<td>" . $row['last3'] . "</td>");
    echo("</tr>");
  }
  echo("</table>");
?>

<?php include("footer.php"); ?>



$result = mysqli_query($conn, "SELECT pageid, title, count(*) as edits 
    from contributions t1 group by pageid, title");


$result = mysqli_query($conn, "SELECT t2.* 
    from editor t1 join contributions t2 on t1.editor_id = t2.editor_id 
    where t1.name='" . $_GET['name'] . "'");

echo("<table border=1>");
while ($row = mysqli_fetch_array($result)) {
  echo("<tr>");
  echo("<td><a href='page.php?pageid=" . $row['pageid'] . "'>" . $row['title'] . "</a></td>");
  echo("<td>" . $row['timestamp'] . "</td>");
  echo("<td>" . $row['comment'] . "</td>");
  echo("</tr>");
}
"""

