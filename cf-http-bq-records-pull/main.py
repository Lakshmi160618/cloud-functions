import functions_framework
from google.cloud import bigquery

@functions_framework.http
def gcf_schedule(request): 
    request_args = request.args  
    if request_args and 'name' in request_args:
        name = request_args['name']
    else:
        return "Name parameter is required", 400
    
    # Initialize a BigQuery client
    client = bigquery.Client()
    
    # Define your query
    query = """
    SELECT *
    FROM `ilovegcp-426017.useeastsouthcarolona.orders_tax`
    WHERE item_name = @name
    """
    
    # Set up query parameters
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("name", "STRING", name)
        ]
    )
    
    # Execute the query
    query_job = client.query(query, job_config=job_config)
    
    # Collect the results
    results = query_job.result()
    
    # Format the results into a list of dictionaries
    rows = [dict(row) for row in results]
    
    # Return the results as JSON
    return {"data": rows}
