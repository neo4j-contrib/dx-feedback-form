import json
from urllib import parse
import boto3
from neo4j import GraphDatabase
from retrying import retry
import datetime

import flask
from flask import render_template

ssmc = boto3.client('ssm')
app = flask.Flask('feedback form')


def get_ssm_param(key):
  resp = ssmc.get_parameter(
    Name=key,
    WithDecryption=True
  )
  return resp['Parameter']['Value']

def str2bool(v):
  return v.lower() in ("yes", "true", "t", "1")

host_port = get_ssm_param('com.neo4j.labs.feedback.dbhostport')
user = get_ssm_param('com.neo4j.labs.feedback.dbuser')
password = get_ssm_param('com.neo4j.labs.feedback.dbpassword')

db_driver = GraphDatabase.driver("bolt+routing://%s" % (host_port), auth=(user, password), max_retry_time=15)

post_feedback_query = """
MERGE (page:Page {uri: $page})
CREATE (feedback:Feedback)
SET feedback += $params, feedback.timestamp = datetime()
CREATE (page)-[:HAS_FEEDBACK]->(feedback)
"""

@retry(stop_max_attempt_number=5, wait_random_max=1000)
def post_feedback(params):
    with db_driver.session() as session:
        result = session.run(post_feedback_query, params)
        print(result.summary().counters)
        return True

def feedback(request, context):
    print("request:", request, "context:", context)

    form_data = parse.parse_qsl(request["body"])

    params = {key:value for key,value in form_data}
    page = params["url"]
    params["helpful"] = str2bool(params["helpful"])
    params["userAgent"] = request["headers"]["User-Agent"]
    params["referer"] = request["headers"]["Referer"]

    print(page, params)

    post_feedback({"params": params, "page": page})

    return {
        "statusCode": 200,
        "body": json.dumps({"message" :"Foo"}),
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
            }
    }

def feedback_page(event, context):
    with db_driver.session() as session:
        result = session.run("""
        MATCH (feedback:Feedback)<-[:HAS_FEEDBACK]-(page)
        RETURN feedback, page
        ORDER BY feedback.timestamp DESC
        LIMIT 50
        """)

        rows = [{"helpful": row["feedback"]["helpful"],
                 "information": row["feedback"]["moreInformation"],
                 "uri": row["page"]["uri"],
                 "timestamp": row["feedback"]["timestamp"].to_native().strftime("%d %b %Y")}
                for row in result]

    with app.app_context():
        rendered = render_template('feedback-form.html', rows=rows)
    response = {
        "statusCode": 200,
        "body": rendered,
        "headers": { 'Content-Type': 'text/html' }
    }

    return response
