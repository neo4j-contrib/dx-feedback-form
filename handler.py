import json
from urllib import parse
import boto3
from neo4j import GraphDatabase
from retrying import retry

ssmc = boto3.client('ssm')


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
    params["helpful"] = str2bool(params["helpful"])
    page = params["url"]

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
