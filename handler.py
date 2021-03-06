import base64
import datetime
import json
import logging
from urllib import parse

import boto3
import flask
from dateutil import parser
from neo4j import GraphDatabase
from retrying import retry

ssmc = boto3.client('ssm')
app = flask.Flask('feedback form')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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

db_driver = GraphDatabase.driver(f"bolt+routing://{host_port}", auth=(user, password), max_retry_time=15)

post_feedback_query = """
MATCH (project:Project {name: $project})
MERGE (page:Page {uri: $page})
MERGE (page)-[:PROJECT]->(project)
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


def determine_project(page):
    if "/docs/labs/neo4j-streams" in page:
        return "neo4j-streams"
    if "grandstack.io" in page:
        return "GRANDstack"
    return "apoc"


def feedback(request, context):
    print("request:", request, "context:", context)

    form_data = parse.parse_qsl(request["body"])

    params = {key: value for key, value in form_data}

    page = params["url"]
    params["helpful"] = str2bool(params["helpful"])

    headers = request["headers"]
    params["userAgent"] = headers.get("User-Agent")
    params["referer"] = headers.get("Referer")

    if "project" in params:
        project = params["project"]
    else:
        project = determine_project(page)

    print(page, params)

    post_feedback({"params": params, "page": page, "project": project})

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Foo"}),
        "headers": {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
        }
    }


def feedback_api(event, context):
    path_parameters = event.get("pathParameters")

    if not path_parameters:
        return {
            "statusCode": 404
        }

    project = path_parameters.get("project").replace("@graphapps-", "@graphapps/")

    qs = event.get("multiValueQueryStringParameters")
    if qs and qs.get("date"):
        now = parser.parse(qs["date"][0])
    else:
        now = datetime.datetime.now().replace(day=1)

    logger.info(f"Retrieving feedback for {now}")

    with db_driver.session() as session:
        params = {"year": now.year, "month": now.month, "project": project}
        result = session.run("""
        MATCH (feedback:Feedback)<-[:HAS_FEEDBACK]-(page)-[:PROJECT]->(:Project {name: $project})
        WHERE datetime({year:$year, month:$month+1}) > feedback.timestamp >= datetime({year:$year, month:$month })
        RETURN feedback, page
        ORDER BY feedback.timestamp DESC
        """, params)

        rows = [{"helpful": row["feedback"]["helpful"],
                 "information": row["feedback"]["moreInformation"],
                 "reason": row["feedback"]["reason"],
                 "uri": row["page"]["uri"],
                 "date": row["feedback"]["timestamp"].to_native().strftime("%d %b %Y")
                 }
                for row in result]

    response = {
        "statusCode": 200,
        "body": json.dumps(rows),
        "headers": {
            "Content-Type": "application/json",
            'Access-Control-Allow-Origin': '*'
        }
    }

    return response


def page_api(event, context):
    logger.info(f"event: {event}, context: {context}")
    path_parameters = event.get("pathParameters")

    if not path_parameters:
        return {
            "statusCode": 404
        }

    encoded_page = path_parameters.get("id")
    page = base64.b64decode(encoded_page).decode("utf-8")

    logger.info(f"page: {page}")
    with db_driver.session() as session:
        params = {"page": page}
        result = session.run("""
        MATCH (page {uri: $page})
        RETURN page, [(page)-[:HAS_FEEDBACK]->(feedback) | feedback] AS feedback
        """, params)

        rows = [{"uri": row["page"]["uri"],
                 "feedback": [{
                     "helpful": entry["helpful"],
                     "information": entry["moreInformation"],
                     "reason": entry["reason"],
                     "date": entry["timestamp"].to_native().strftime("%d %b %Y")
                 }
                     for entry in row["feedback"]
                 ]}
                for row in result]

    response = {
        "statusCode": 200,
        "body": json.dumps(rows),
        "headers": {
            "Content-Type": "application/json",
            'Access-Control-Allow-Origin': '*'
        }
    }

    return response


def fire_api(event, context):
    path_parameters = event.get("pathParameters")

    if not path_parameters:
        return {
            "statusCode": 404
        }

    project = path_parameters.get("project").replace("@graphapps-", "@graphapps/")

    with db_driver.session() as session:
        result = session.run("""
        MATCH (project:Project {name: $project})<-[:PROJECT]-(page:Page)-[:HAS_FEEDBACK]->(feedback)
        WITH page, collect(feedback) AS allFeedback
        WITH page,
             size([f in allFeedback WHERE f.helpful]) AS helpful,
             size([f in allFeedback WHERE not(f.helpful)]) AS notHelpful
        WHERE notHelpful > 0

        WITH page, helpful, notHelpful,
        helpful + notHelpful * 1.0 AS n
        WITH page, helpful, notHelpful, n,
        1.281551565545 AS z,
        (notHelpful * 1.0) / n AS p
        WITH page, helpful, notHelpful, n, z, p,
        p + (1.0/(2*n))*(z*z) AS left,
        z*(sqrt((p*(1-p)/n) + (z*z)/(4*n*n))) AS right,
        1+(1/n*z*z) AS under
        RETURN page, notHelpful, helpful, (left-right) / under AS unhelpfulness
        ORDER BY unhelpfulness desc
        """, {"project": project})

        rows = [{"uri": row["page"]["uri"],
                 "helpful": row["helpful"],
                 "notHelpful": row["notHelpful"],
                 "unhelpfulness": row["unhelpfulness"]
                 }
                for row in result]

    response = {
        "statusCode": 200,
        "body": json.dumps(rows),
        "headers": {
            "Content-Type": "application/json",
            'Access-Control-Allow-Origin': '*'
        }
    }

    return response
