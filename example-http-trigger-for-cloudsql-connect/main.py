import os
import pymysql


def http_trigger(request):
    # Set the Unix socket path for Cloud SQL Proxy
    unix_socket = '/cloudsql/lct-dev-416808:asia-south1:lct-mysql-dev'
    
    # Configure the MySQL connection
    connection = pymysql.connect(
        user='root',
        password='Lavu@1234',
        database='customers',
        unix_socket=unix_socket,
        cursorclass=pymysql.cursors.DictCursor
    )
    
    # Execute a sample SQL query
    with connection.cursor() as cursor:
        sql = "select * from Customers;"
        cursor.execute(sql)
        result = cursor.fetchall()
        print(result)
    
    # Close the MySQL connection
    connection.close()
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return f'Hello World!'