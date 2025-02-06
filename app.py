# External libraries
import os,json
import requests
import pymysql

import mysql.connector
from flask import Flask, jsonify, request, abort,redirect, url_for, make_response
# from hdbcli import dbapi
from datetime import datetime
from datetime import date
# from ordereddict import *
# from crud_d4d import *
import pandas as pd
import uuid

from sap import xssec

from flask_cors import CORS, cross_origin
from cfenv import AppEnv


# Class initiated to use methods and features
# app = Flask(__name__)
# CORS(app)


env = AppEnv()

app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@cross_origin()

#Function to redirect urls
@app.route('/<string:function>', methods=['GET'])
def home(function):
    if function == 'read':
        return redirect(url_for('read'))
    elif function == 'create':
        return redirect(url_for('create'))
    elif function == 'modify':
        return redirect(url_for('modify'))
    elif function == 'record':
        return redirect(url_for('record'))
    elif function == 'layoutmodify':
        return redirect(url_for('layoutmodify'))
    elif function == 'layoutDelete':
        return redirect(url_for('layoutDelete'))

     # Custom Tab Changes
    elif function == 'customtab_read':
        return redirect(url_for('customtab_read'))
    elif function == 'customtab_write':
        return redirect(url_for('customtab_write'))    
    
    elif function == 'userHistory':              
        return redirect(url_for('userHistory'))

    elif function == 'get_user_master':              
        return redirect(url_for('get_user_master'))
    
    elif function == 'upsert_user_master':              
        return redirect(url_for('upsert_user_master'))
    
    
    elif function == 'layoutmodifybatch':
        return redirect(url_for('layoutmodifybatch'))

    elif function == 'dataWrite':
        return redirect(url_for('dataWrite'))
    elif function == 'dataRead':
        return redirect(url_for('dataRead'))
    elif function == 'feedbackWrite':
        return redirect(url_for('feedbackWrite'))
    elif function == 'dataReadDYK':
        return redirect(url_for('dataReadDYK'))
    elif function == 'get_suggestions':              
        return redirect(url_for('get_suggestions'))
    elif function == "get_recent_prompts":
        return redirect(url_for('get_recent_prompts'))
    else:
        return jsonify({"error": "Try again"})

# Function to connect to MySQL
def connection_mysql():
    db_config = {
    'host': '10.25.77.251',
    'port': 3306,
    'database': 'ZORAUIUXDB',
    'user': 'root',
    'password': 'a0k2MnVUaElLYWphc3Qx'
}
 
    conn = pymysql.connect(**db_config)
    return conn

# Function to handle errors
def handle_error(e):
    response = make_response(jsonify({"error": str(e)}), 500)
    return response

# Function to insert / update records in refresh table on publish functionality in UX 
@app.route("/dataWrite2", methods=['GET', 'POST'])
def interfaceWritetemp():
    try:

        data = request.get_json()
        data = json.dumps(data)
        data = json.loads(data)
        
        conn = connection_mysql()
        cursor = conn.cursor()
        
        existing = []
        new = []

        queries = []
        for row in data:
            queries.append(row['query'].strip())

        queries = set(queries)

        check = existing_records(queries)
        # length = len(check)

        for row in data:
            if row['query'] in check:
                chart_metadata = str(row['chart_metadata'])
                col = str(row['chart_cols'])
                col1 = str(row['chart_cols1'])
                chart_data = str(row['chart_data'])
                chart_data1 = str(row['chart_data1']) if 'chart_data1' in row else ''
                chart_data2 = str(row['chart_data2']) if 'chart_data2' in row else ''
                chart_data3 = str(row['chart_data3']) if 'chart_data3' in row else ''
                message = str(row['chart_message'])
                message1 = message[:4999]
                message2 = message[4999:9999]
                message3 = message[9999:14998]
                headline = row['headline']
                cagr_string = row['cagr_string']
                query = row['query']
                current_timestamp = datetime.now()
                parent_answer_ner = row['parent_answer_ner'] if 'parent_answer_ner' in row else ''
                parent_question_ner = row['parent_question_ner'] if 'parent_question_ner' in row else ''
                time_suffix = row['time_suffix'] if 'time_suffix' in row else ''
                chart_title = row['chart_title'] if 'chart_title' in row else ''
                work_area = row['work_area'] if 'work_area' in row else ''
                existing.append([chart_metadata, col, col1, chart_data, chart_data1, chart_data2, chart_data3, message1, message2, message3, cagr_string, headline, current_timestamp, str(parent_question_ner), parent_answer_ner, time_suffix, chart_title, work_area, query])
            else:
                chart_metadata = str(row['chart_metadata'])
                col = str(row['chart_cols'])
                col1 = str(row['chart_cols1'])
                chart_data = str(row['chart_data'])
                chart_data1 = str(row['chart_data1']) if 'chart_data1' in row else ''
                chart_data2 = str(row['chart_data2']) if 'chart_data2' in row else ''
                chart_data3 = str(row['chart_data3']) if 'chart_data3' in row else ''
                message = str(row['chart_message'])
                message1 = message[:4999]
                message2 = message[4999:9999]
                message3 = message[9999:14998]
                headline = row['headline']
                cagr_string = row['cagr_string']
                query = row['query']
                current_timestamp = datetime.now()
                parent_answer_ner = row['parent_answer_ner'] if 'parent_answer_ner' in row else ''
                parent_question_ner = row['parent_question_ner'] if 'parent_question_ner' in row else ''
                time_suffix = row['time_suffix'] if 'time_suffix' in row else ''
                chart_title = row['chart_title'] if 'chart_title' in row else ''
                work_area = row['work_area'] if 'work_area' in row else ''
                new.append([query, chart_metadata, col, col1, chart_data, chart_data1, chart_data2, chart_data3, message1, message2, message3, cagr_string, headline, current_timestamp, current_timestamp, str(parent_question_ner), parent_answer_ner, time_suffix, chart_title, work_area])

        if new:
            insert_sql = """INSERT INTO ZT_NEWSLETTER_REFRESH (
             QUERY,
             CHART_METADATA,
             CHART_COLS,
             CHART_COLS1,
             CHART_DATA,
             CHART_DATA1,
             CHART_DATA2,
             CHART_DATA3,
             CHART_MESSAGE1,
             CHART_MESSAGE2,
             CHART_MESSAGE3,
             CAGR_STRING,
             HEADLINE,
             ADDED_ON,
             REFRESHED_ON,
             PARENT_QUESTION_NER,
             PARENT_ANSWER_NER,
             TIME_SUFFIX,
             CHART_TITLE,
        WORK_AREA) VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            cursor.executemany(insert_sql, new)
            # print('Data inserted successfully')

        if existing:
            update_sql = """UPDATE ZT_NEWSLETTER_REFRESH SET
                CHART_METADATA = %s,
                CHART_COLS = %s,
                CHART_COLS1 = %s,
                CHART_DATA = %s,
                CHART_DATA1 = %s,
                CHART_DATA2 = %s,
                CHART_DATA3 = %s,
                CHART_MESSAGE1 = %s,
                CHART_MESSAGE2 = %s,
                CHART_MESSAGE3 = %s,
                CAGR_STRING = %s,
                HEADLINE = %s,
                REFRESHED_ON = %s,
                PARENT_QUESTION_NER = %s,
                PARENT_ANSWER_NER = %s,
                TIME_SUFFIX = %s,
                CHART_TITLE = %s,
                WORK_AREA = %s
                WHERE
                 QUERY = %s;"""

            cursor.executemany(update_sql, existing)
            # print('Data updated successfully')
        conn.close()
        return jsonify({"response": 'Data inserted successfully'})
    except Exception as e :
        return handle_error(e)
        
@app.route("/dataWrite", methods=['GET', 'POST'])
def interfaceWrite():
    try:
        data = request.get_json()
        data = json.dumps(data)
        data = json.loads(data)
        
        conn = connection_mysql()
        cursor = conn.cursor()
        
        existing = []
        new = []

        queries = [row['query'].strip() for row in data]
        queries = set(queries)

        check = existing_records(queries)

        for row in data:
            chart_metadata = json.dumps(row['chart_metadata'])
            col = json.dumps(row['chart_cols'])
            col1 = json.dumps(row['chart_cols1'])
            chart_data = json.dumps(row['chart_data'])
            chart_data1 = json.dumps(row['chart_data1']) if 'chart_data1' in row else ''
            chart_data2 = json.dumps(row['chart_data2']) if 'chart_data2' in row else ''
            chart_data3 = json.dumps(row['chart_data3']) if 'chart_data3' in row else ''
            message = row['chart_message']
            message1 = message[:4999]
            message2 = message[4999:9999]
            message3 = message[9999:14998]
            headline = row['headline']
            cagr_string = row['cagr_string']
            query = row['query']
            current_timestamp = datetime.now()
            parent_answer_ner = json.dumps(row['parent_answer_ner']) if 'parent_answer_ner' in row else ''
            parent_question_ner = json.dumps(row['parent_question_ner']) if 'parent_question_ner' in row else ''
            time_suffix = row['time_suffix'] if 'time_suffix' in row else ''
            chart_title = row['chart_title'] if 'chart_title' in row else ''
            work_area = row['work_area'] if 'work_area' in row else ''

            if query in check:
                update_sql = """UPDATE ZT_NEWSLETTER_REFRESH SET
                    CHART_METADATA = %s,
                    CHART_COLS = %s,
                    CHART_COLS1 = %s,
                    CHART_DATA = %s,
                    CHART_DATA1 = %s,
                    CHART_DATA2 = %s,
                    CHART_DATA3 = %s,
                    CHART_MESSAGE1 = %s,
                    CHART_MESSAGE2 = %s,
                    CHART_MESSAGE3 = %s,
                    CAGR_STRING = %s,
                    HEADLINE = %s,
                    REFRESHED_ON = %s,
                    PARENT_QUESTION_NER = %s,
                    PARENT_ANSWER_NER = %s,
                    TIME_SUFFIX = %s,
                    CHART_TITLE = %s,
                    WORK_AREA = %s
                    WHERE QUERY = %s"""
                cursor.execute(update_sql, (chart_metadata, col, col1, chart_data, chart_data1, chart_data2, chart_data3, message1, message2, message3, cagr_string, headline, current_timestamp, parent_question_ner, parent_answer_ner, time_suffix, chart_title, work_area, query))
            # return jsonify({"response": 'Data updated successfully'})
            else:
                insert_sql = """INSERT INTO ZT_NEWSLETTER_REFRESH (
                    QUERY,
                    CHART_METADATA,
                    CHART_COLS,
                    CHART_COLS1,
                    CHART_DATA,
                    CHART_DATA1,
                    CHART_DATA2,
                    CHART_DATA3,
                    CHART_MESSAGE1,
                    CHART_MESSAGE2,
                    CHART_MESSAGE3,
                    CAGR_STRING,
                    HEADLINE,
                    ADDED_ON,
                    REFRESHED_ON,
                    PARENT_QUESTION_NER,
                    PARENT_ANSWER_NER,
                    TIME_SUFFIX,
                    CHART_TITLE,
                    WORK_AREA) VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(insert_sql, (query, chart_metadata, col, col1, chart_data, chart_data1, chart_data2, chart_data3, message1, message2, message3, cagr_string, headline, current_timestamp, current_timestamp, parent_question_ner, parent_answer_ner, time_suffix, chart_title, work_area))
        
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"response": 'Data inserted/Updated successfully'})
    except Exception as e:
        return jsonify({"error": str(e)})

# Function to check existing records in database
def existing_records_temp(queries):
    try:
        conn = connection_mysql()
        cursor = conn.cursor()
        sql = 'SELECT DISTINCT(TRIM("QUERY")) FROM ZT_NEWSLETTER_REFRESH'
        cursor.executemany(sql)
        output = cursor.fetchall()
        cursor.close()
        conn.close()
        # final_result = list(map(str, output))
        final_result = [i[0] for i in output]   
        exists = []
        for i in final_result:
            if i in queries:
                exists.append(i)
        return exists
    except Exception as e :
        return []

def existing_records(queries):
    try:
        conn = connection_mysql()
        cursor = conn.cursor()
        sql = 'SELECT DISTINCT(TRIM(`QUERY`)) FROM ZT_NEWSLETTER_REFRESH'
        cursor.execute(sql)
        output = cursor.fetchall()
        cursor.close()
        conn.close()
        final_result = [i[0] for i in output]
        exists = [i for i in final_result if i in queries]
        return exists
    except Exception as e:
        print(f"Error: {e}")
        return []

def readBySingleFilter(cursor, sql, filter):
    cursor.execute(sql, filter)
    rows = cursor.fetchall()
    headers = [i[0] for i in cursor.description]
    df = pd.DataFrame(rows, columns=headers)
    result = df.to_json(orient="records", indent = 4, force_ascii=False)
    return result

# Function to read suggestions data for prompt bar in UX landing page
@app.route("/get_suggestions/<string:user>", methods = ['GET'])
def get_top5_suggestions(user):
    try:
        conn = connection_mysql()
        cursor = conn.cursor()
        readSuggestions_sql = """
        SELECT DISTINCT USER, TRIM(QUERY) AS QUERY, COUNT
            FROM ZT_NEWSLETTER_SUGGESTIONS AS A
            WHERE LOWER(USER) = %s
            AND NOT EXISTS (
            SELECT DISTINCT USER, TRIM(QUERY) AS QUERY
            FROM ZT_NEWSLETTER_LAYOUT AS B
            WHERE B.QUERY = TRIM(A.QUERY)
            AND LOWER(B.USER) = LOWER(A.USER)
                )
            GROUP BY USER, QUERY, COUNT, ADDED_ON
            ORDER BY COUNT DESC
            LIMIT 5;"""
        readSuggestions = readBySingleFilter(cursor, readSuggestions_sql, user)
        
        return readSuggestions
      
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()

# Get last 5 prompts searched by user in UX
@app.route("/get_recent_prompts/<string:user>", methods = ['GET'])
def get_recent_prompts(user):
    try:
        conn = connection_mysql()
        cursor = conn.cursor()
        read_sql = """
        SELECT QUERY, LATEST_ADDED_ON FROM (
             SELECT DISTINCT USER, QUERY, COUNT(QUERY) AS QUERY_COUNT, MAX(ADDED_ON_DATETIME) AS LATEST_ADDED_ON
                FROM ZT_NEWSLETTER_USERHISTORY
                WHERE LOWER(USER) = %s
                GROUP BY USER, QUERY
                ORDER BY LATEST_ADDED_ON DESC
                LIMIT 5
                ) AS subquery;"""
        cursor.execute(read_sql,(user,))
        rows = cursor.fetchall()
        headers = [i[0] for i in cursor.description]
        df = pd.DataFrame(rows, columns=headers)
        result = df.to_json(orient="records", indent = 4, force_ascii=False)
        return result
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()

def safe_json_loads(x):
    try:
        return json.loads(x)
    except (KeyError):
        pass
    except (TypeError, ValueError, json.JSONDecodeError):
        try:
            return json.loads(x.replace("'", '"'))
        except (TypeError, ValueError, json.JSONDecodeError):
            return x

# Function to read data for Did You Know section in UX
@app.route("/dataReadDYK", methods=['GET', 'POST'])
def dataReadDYK():
    try:
        conn = connection_mysql()
        cursor = conn.cursor()

        sql = """SELECT
                USE_CASE,
                EXPENSE_CATEGORY,
                METRICS,
                KPI_LEVEL,
                KPI_VALUE,
                FINAL_ANSWER1,
                FINAL_ANSWER2,
                HEADLINE,
                REFRESHED_ON,
                CHART_METADATA,
                CHART_COLS,
                CHART_COLS1,
                CHART_DATA
                FROM ZT_NEWSLETTER_DYK
                ORDER BY USE_CASE DESC;"""

        cursor.execute(sql)
        output = cursor.fetchall()
        headers = [i[0] for i in cursor.description]
        cursor.close()
        conn.close()

        df = pd.DataFrame(output, columns=headers)
        df['FINAL_ANSWER'] = df['FINAL_ANSWER1'] + df['FINAL_ANSWER2']
        df = df.drop(columns=['FINAL_ANSWER1', 'FINAL_ANSWER2', 'REFRESHED_ON'])
        columns_to_convert = ['CHART_METADATA', 'CHART_COLS', 'CHART_COLS1', 'CHART_DATA']
        df[columns_to_convert] = df[columns_to_convert].applymap(safe_json_loads)
        # print(df)
        json_data = df.to_json(orient='records', indent=4)
        response = make_response(json_data)
        response.status_code = 200
        # response = json_data
        return response
    except Exception as e :
        return handle_error(e)

# Function to get insights data stored by users --> to be displayed in UX tabs 
@app.route("/dataRead/<string:user>", methods = ['GET'])
def dataRead(user):
    try:
        conn = connection_mysql()
        cursor = conn.cursor()
        sql = """
        SELECT * FROM
    (
    SELECT
    ROW_NUMBER( ) OVER ( PARTITION BY QUERY, CATEGORY_NAME order by ADDED_ON DESC) AS ROW_ID,
    QUERY,
    CHART_METADATA,
    CHART_COLS,
    CHART_COLS1,
    CHART_DATA,
    CHART_DATA1,
    CHART_DATA2,
    CHART_DATA3,
    CHART_MESSAGE1,
    CHART_MESSAGE2,
    CHART_MESSAGE3,
    CAGR_STRING,
    HEADLINE,
    PARENT_QUESTION_NER as "parent_question_ner",
    PARENT_ANSWER_NER as "parent_answer_ner",
    CHART_TITLE,
    WORK_AREA,
    POSITION,
    USER,
    ADDED_ON,
    LAST_MODIFIED_ON,
    VIZ_TYPE,
    COMMENTS,
    CATEGORY_NAME
    FROM (
        SELECT DISTINCT 
        TRIM(ZT_NEWSLETTER_REFRESH.QUERY) AS QUERY,
        ZT_NEWSLETTER_REFRESH.CHART_METADATA,
        ZT_NEWSLETTER_REFRESH.CHART_COLS,
        ZT_NEWSLETTER_REFRESH.CHART_COLS1,
        ZT_NEWSLETTER_REFRESH.CHART_DATA,
        ZT_NEWSLETTER_REFRESH.CHART_DATA1,
        ZT_NEWSLETTER_REFRESH.CHART_DATA2,
        ZT_NEWSLETTER_REFRESH.CHART_DATA3,
        ZT_NEWSLETTER_REFRESH.CHART_MESSAGE1,
        ZT_NEWSLETTER_REFRESH.CHART_MESSAGE2,
        ZT_NEWSLETTER_REFRESH.CHART_MESSAGE3,
        ZT_NEWSLETTER_REFRESH.CAGR_STRING,
        ZT_NEWSLETTER_REFRESH.HEADLINE,
        ZT_NEWSLETTER_REFRESH.PARENT_QUESTION_NER,
        ZT_NEWSLETTER_REFRESH.PARENT_ANSWER_NER,
        ZT_NEWSLETTER_REFRESH.CHART_TITLE,
        ZT_NEWSLETTER_REFRESH.WORK_AREA,
        ZT_NEWSLETTER_LAYOUT.POSITION,
        ZT_NEWSLETTER_LAYOUT.USER,
        ZT_NEWSLETTER_LAYOUT.ADDED_ON,
        ZT_NEWSLETTER_LAYOUT.LAST_MODIFIED_ON,
        ZT_NEWSLETTER_LAYOUT.VIZ_TYPE,
        ZT_NEWSLETTER_LAYOUT.COMMENTS,
        ZT_NEWSLETTER_LAYOUT.CATEGORY_NAME
        FROM 
            ZT_NEWSLETTER_REFRESH
        INNER JOIN
            ZT_NEWSLETTER_LAYOUT
        ON ZT_NEWSLETTER_LAYOUT.QUERY = ZT_NEWSLETTER_REFRESH.QUERY
        WHERE ZT_NEWSLETTER_LAYOUT.USER = %s
        ) AS subquery
    ) AS finalquery
    WHERE ROW_ID = 1
    """
        cursor.execute(sql,(user,))
        output = cursor.fetchall()
        headers = [i[0] for i in cursor.description]

        cursor.close()
        conn.close()
        
        df = pd.DataFrame(output, columns=headers)
        df['CHART_MESSAGE'] = df['CHART_MESSAGE1'] + df['CHART_MESSAGE2'] + df['CHART_MESSAGE3']
        df = df.drop(columns=['CHART_MESSAGE1', 'CHART_MESSAGE2', 'CHART_MESSAGE3'])
        
        columns_to_convert = ['CHART_METADATA', 'CHART_COLS', 'CHART_COLS1', 'CHART_DATA', 'CHART_DATA1', 'CHART_DATA2', 'CHART_DATA3', 'parent_question_ner']
        df[columns_to_convert] = df[columns_to_convert].applymap(safe_json_loads)
        # print(df)
        json_data = df.to_json(orient='records', indent=4)
        # print(json_data)

        return json_data
    except Exception as e :
        return jsonify({"Error occured while retrieving data"})

# Function to write feedback provided by users -> binded to chatbot feedback in UX
@app.route("/feedbackWrite", methods=['POST'])
def insertFeedback():
    try:
        
#         data = {
#     "user": "Ace",
#     "origin": "Weddb",
#     "query": "What is AI?",
#     "user_query": "How does AI work?",
#     "ai_response": "AI stands for Artificial Intelligence, which is the simulation of human intelligence in machines.",
#     "feedback_type": "Positive",
#     "comments": "The response was very helpful.",
#     "added_on": "2025-02-04 22:00:00"
# }
        data = request.get_json()
        user = data['user']
        origin = data['origin']
        query = data['query']
        user_query = data['user_query']
        ai_response = data['ai_response']
        feedback_type = data['feedback_type']
        comments = data['comments']
        current_timestamp = datetime.now()

        values = (user, origin, query, user_query, ai_response, feedback_type, comments, current_timestamp)

        conn =connection_mysql()
        cursor = conn.cursor()

        insert_sql = """INSERT INTO ZT_NEWSLETTER_FEEDBACK (USER, ORIGIN, QUERY, USER_QUERY, AI_RESPONSE, FEEDBACK_TYPE, COMMENTS, ADDED_ON)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        cursor.execute(insert_sql, values)
        conn.commit()
        conn.close()
        return jsonify({"status": "success", "message": "Feedback added successfully"})
    except Exception as e:
        return handle_error(e)
        
# Function to CREATE
@app.route("/create", methods=['POST'])
def create():
    try:
        data = request.get_json()
        conn = connection_mysql()
        current_date = date.today().strftime("%Y-%m-%d")
        cursor = conn.cursor()
        sql = 'INSERT INTO ZT_NEWSLETTER_LAYOUT ("POSITION","USER","QUERY","VIZ_TYPE","COMMENTS","CATEGORY_NAME","LAST_MODIFIED_ON","ADDED_ON") VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        for dicc in data:
            values = [dicc["POSITION"], dicc["USER"], dicc["QUERY"], dicc["VIZ_TYPE"], dicc["COMMENTS"], dicc["CATEGORY_NAME"], current_date, current_date]
            cursor.execute(sql, values)
        conn.commit()
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()
    return jsonify({"create": "done"})

# Function to READ
@app.route("/read/<string:user>", methods = ['GET'])
def read(user):
    # try:
    # print("Connecting to DB")
    conn = connection_mysql()
    # print(conn)
    cursor_temp = conn.cursor()
    sql = 'SELECT * FROM ZT_NEWSLETTER_LAYOUT WHERE USER = %s ORDER BY POSITION ASC'
    cursor_temp.execute(sql, (user,))
    # cursor_temp.execute(sql)
    output = cursor_temp.fetchall()
    info = []
    for row in output:
        info.append({"POSITION": row[0], "USER": row[1], "QUERY": row[2], "VIZ_TYPE": row[3], "COMMENTS": row[4], "CATEGORY_NAME": row[5], "LAST_MODIFIED_ON": row[6].strftime("%Y-%m-%d"), "ADDED_ON": row[7].strftime("%Y-%m-%d")})
    # except Exception as e:
    #     return handle_error(e)
    # finally:
    cursor_temp.close()
    conn.close()
    return jsonify(info)

# Function to MODIFY (update)
# solo se modifica position
@app.route("/modify", methods =['POST'])
def modify():
    try:
        data = request.get_json()
        conn = connection_mysql()
        current_date = date.today()
        cursor = conn.cursor()
        sql = 'UPDATE ZT_NEWSLETTER_LAYOUT SET "POSITION" = ?, LAST_MODIFIED_ON = ? WHERE "USER" = ? AND "QUERY" = ?'
        for dicc in data:
            values = [dicc["POSITION"], current_date, dicc["USER"], dicc["QUERY"]]
            cursor.execute(sql, values)
        conn.commit()
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()
    return jsonify({"modify": "done"})

# Function to RECORD
@app.route("/record/<string:user>", methods = ['GET'])
def record(user):
    try:
        conn = connection_mysql()
        cursor = conn.cursor()
        sql = 'SELECT count("USER") FROM ZT_NEWSLETTER_LAYOUT WHERE "USER" = ?'
        cursor.execute(sql, (user,))
        output = cursor.fetchall()
        info = []
        for row in output:
            info.append({"USER": user, "No. RECORDS": row[0]})
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()
    return jsonify(info)

# Function to LAYOUTMODIFY layoutmodify
@app.route("/layoutmodify/<string:user>/<string:category>", methods=['POST'])
def layoutmodify(user, category):
    try:
#         data=[
#     {
#         "POSITION": 1,
#         "USER": "Ace",
#         "QUERY": "How to optimize database queries?",
#         "VIZ_TYPE": "Bar Chart",
#         "COMMENTS": "Important query optimization techniques",
#         "CATEGORY_NAME": "Database11"
#     },
#     {
#         "POSITION": 2,
#         "USER": "Ace",
#         "QUERY": "Best practices for securing web applications",
#         "VIZ_TYPE": "Pie Chart",
#         "COMMENTS": "Useful security tips",
#         "CATEGORY_NAME": "Security133"
#     }
# ]

        current_date = date.today().strftime("%Y-%m-%d")
        conn=connection_mysql()
        cursor = conn.cursor()
        

        # FIRST STEP
        sql_delete = 'DELETE FROM ZT_NEWSLETTER_LAYOUT WHERE USER = %s AND CATEGORY_NAME = %s'
        parameters = (user, category)
        cursor.execute(sql_delete, parameters)

        # SECOND STEP
        # data = request.get_json()
        sql = 'INSERT INTO ZT_NEWSLETTER_LAYOUT (POSITION, USER, QUERY, VIZ_TYPE, COMMENTS, CATEGORY_NAME, LAST_MODIFIED_ON, ADDED_ON) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
        for dicc in data:
            values = (dicc["POSITION"], dicc["USER"], dicc["QUERY"], dicc["VIZ_TYPE"], dicc["COMMENTS"], dicc["CATEGORY_NAME"], current_date, current_date)
            cursor.execute(sql, values)
        conn.commit()
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()
    return jsonify({"layoutmodify": "done"})

# Function to LAYOUTMODIFY layoutmodify
@app.route("/layoutmodifybatch/<string:user>", methods=['POST'])
def layoutmodifybatch(user):
    try:
#         data=[
#     {
#         "POSITION": 1,
#         "USER": "Ace",
#         "QUERY": "How to optimizerrr database queries?",
#         "VIZ_TYPE": "Bar Chart",
#         "COMMENTS": "Important query optimization techniques",
#         "CATEGORY_NAME": "Database"
#     },
#     {
#         "POSITION": 2,
#         "USER": "Ace",
#         "QUERY": "Best practices for ring web applications",
#         "VIZ_TYPE": "Pie Chart",
#         "COMMENTS": "Useful security tips",
#         "CATEGORY_NAME": "Security"
#     }
# ]
    
        current_date = date.today().strftime("%Y-%m-%d")
        conn=connection_mysql()
        cursor = conn.cursor()
        data = request.get_json()
        categories = sorted(set([dicc["CATEGORY_NAME"] for dicc in data]))

        sql_insert = 'INSERT INTO ZT_NEWSLETTER_LAYOUT (POSITION, USER, QUERY, VIZ_TYPE, COMMENTS, CATEGORY_NAME, LAST_MODIFIED_ON, ADDED_ON) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'

        sql_delete = 'DELETE FROM ZT_NEWSLETTER_LAYOUT WHERE USER = %s AND CATEGORY_NAME = %s'

        for category in categories:
            values = []
            for dicc in data:
                if dicc["CATEGORY_NAME"] == category:
                    values.append((dicc["POSITION"], dicc["USER"], dicc["QUERY"], dicc["VIZ_TYPE"], dicc["COMMENTS"], dicc["CATEGORY_NAME"], current_date, current_date))
            
            parameters = (user, category)
            
            if values:
                cursor.execute(sql_delete, parameters)
                cursor.executemany(sql_insert, values)
      
        conn.commit()
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()
    return jsonify({"layoutmodifybatch" : "done"})


# Function to delete layout based on categories
@app.route("/layoutDelete/<string:user>", methods=['POST'])
def layoutDelete(user):
    try:
        
        # data = [
        #     {"category_name": "Database"},
        #     {"category_name": "Health"}
        # ]
        data = request.get_json()
        categories = [dicc["category_name"] for dicc in data]
        values = []
        for category in categories:
            value = (user, category) 
            values.append(value)
        
        conn = connection_mysql()
        cursor = conn.cursor()

        sql_delete = 'DELETE FROM ZT_NEWSLETTER_LAYOUT WHERE USER = %s AND CATEGORY_NAME = %s'
        cursor.executemany(sql_delete, values)
        conn.commit()
    except Exception as e:
        return jsonify({"layoutDelete" : "API failed to delete layout based on categories: " + str(e)})
    finally:
        cursor.close()
        conn.close()
    return jsonify({"layoutDelete" : "done"})


#For Storing Using History on Generate Button
@app.route("/userHistory", methods =['POST'])
def userHistory():
    try:
        # Get the JSON File
        data = request.get_json()

        # Open the connection to MySql DB
        conn = connection_mysql()
        current_date = datetime.now()
        cursor = conn.cursor()
        sql = 'INSERT INTO ZT_NEWSLETTER_USERHISTORY (USER, QUERY, ADDED_ON_DATETIME) VALUES (%s, %s, %s)'
        values = (data["USER"], data["QUERY"], current_date)
        cursor.execute(sql, values)
        conn.commit()
    except Exception as e:
        return handle_error(e)
    finally:
        # Close the Open Connections
        cursor.close()
        conn.close()
    return jsonify({"Message": "User History Saved"})
 

# Custom Tab chnages 
@app.route("/customtab_write_temp/<string:user>", methods=['POST'])
def custom_write_temp(user = None):
    try:
        data = request.get_json()
        conn = connection_mysql()
        current_date = datetime.now()
        cursor = conn.cursor()
        ## FIRST STEP
        sql_delete = 'DELETE FROM ZT_NEWSLETTER_CUSTCATEGORIES WHERE "USER_NAME" = ?'
        cursor.execute(sql_delete, user)
        ## SECOND STEP
        data = request.get_json()
        sql_rewrite = 'INSERT INTO ZT_NEWSLETTER_CUSTCATEGORIES ("POSITION","USER_NAME","TAB_TECH_NAME","TAB_NAME","ADDED_ON_DATE") VALUES (?, ?, ?, ?, ?)'
        for dicc in data:
            values = [dicc["POSITION"], dicc["USER_NAME"], dicc["TAB_TECH_NAME"], dicc["TAB_NAME"], current_date]
            cursor.execute(sql_rewrite, values)
        conn.commit()
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()
    return jsonify({"custom_write" : "done"})
    
@app.route("/customtab_write/<string:user>", methods=['POST'])
def custom_write(user=None):
    try:
        data = request.get_json()
        conn = connection_mysql()
        current_date = datetime.now()
        cursor = conn.cursor()
        
        ## FIRST STEP: Delete existing entries
        sql_delete = 'DELETE FROM ZT_NEWSLETTER_CUSTCATEGORIES WHERE USER_NAME = %s'
        cursor.execute(sql_delete, (user,))
        
        ## SECOND STEP: Insert new entries
        sql_rewrite = 'INSERT INTO ZT_NEWSLETTER_CUSTCATEGORIES (POSITION, USER_NAME, TAB_TECH_NAME, TAB_NAME, ADDED_ON_DATE) VALUES (%s, %s, %s, %s, %s)'
        values = [(dicc["POSITION"], dicc["USER_NAME"], dicc["TAB_TECH_NAME"], dicc["TAB_NAME"], current_date) for dicc in data]
        cursor.executemany(sql_rewrite, values)
        
        conn.commit()
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()
    return jsonify({"custom_write": "done"})

@app.route("/customtab_read/<string:user>", methods = ['GET'])
def custom_read(user):
    try:
        conn = connection_mysql()
        cursor = conn.cursor()
        sql = 'SELECT * FROM ZT_NEWSLETTER_CUSTCATEGORIES WHERE USER_NAME = %s'
        cursor.execute(sql, (user,))
        output = cursor.fetchall()
        info = []
        for row in output:
            order = dict([("POSITION", row[0]), ("USER_NAME", row[1]), ("TAB_TECH_NAME", row[2]), ("TAB_NAME", row[3]), ("ADDED_ON_DATE", row[4].strftime("%Y-%m-%d"))])
            info.append(order)
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()
    response = app.response_class(response=json.dumps(info), mimetype='application/json')
    return response

@app.route("/get_user_master/<string:user>", methods = ['GET'])
def user_master_read(user):
    try:
        conn = connection_mysql()
        cursor = conn.cursor()
        sql = 'SELECT * FROM ZT_NEWSLETTER_USER_MASTER WHERE USER = %s'
        cursor.execute(sql, (user,))
        output = cursor.fetchall()
        headers = [i[0] for i in cursor.description]
        info = []
        for row in output:
            result = dict([("USER", row[0]), ("THEME", row[1]), ("UPDATED_ON", row[2].strftime("%Y-%m-%d")), ("GUID", row[3])])
            info.append(result)
        response = app.response_class(response=json.dumps(info), mimetype='application/json')
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()
    return response

@app.route("/upsert_user_master_temp", methods = ['POST'])
def user_master_write_temp():
    try:
        data = request.get_json()
        current_date = date.today().strftime("%Y-%m-%d")
        for dicc in data:
            values = [dicc["user"]]
        conn = connection_mysql()
        cursor = conn.cursor()

        select_sql = 'SELECT "GUID" FROM ZT_NEWSLETTER_USER_MASTER WHERE "USER" = ?'
        cursor.execute(select_sql, (values[0],))
        output = cursor.fetchone()

        guid = output[0] if output else None

        values = []
        if guid:
            for dicc in data:
                values = [dicc["user"], dicc["theme"], current_date, guid, dicc["user"]]
        else:
            new_guid = str(uuid.uuid4().hex)
            for dicc in data:
                values = [dicc["user"], dicc["theme"], current_date, new_guid, dicc["user"]]

        upsert_sql = 'INSERT INTO ZT_NEWSLETTER_USER_MASTER VALUES (?, ?, ?, ?) where "USER" = ?'
        cursor.execute(upsert_sql, values)

        conn.commit()
    except Exception as e:
        return handle_error(e)
    finally:
        cursor.close()
        conn.close()
    return jsonify({"upsert_user_master": "done"})
    
@app.route("/upsert_user_master", methods=['POST'])
def user_master_write():
    try:
        data = request.get_json()
        current_date = date.today().strftime("%Y-%m-%d")
        
        conn = connection_mysql()
        cursor = conn.cursor()

        # Fetch GUID if USER already exists
        select_sql = 'SELECT GUID FROM ZT_NEWSLETTER_USER_MASTER WHERE USER = %s'
        cursor.execute(select_sql, (data[0]["user"],))
        output = cursor.fetchone()

        guid = output[0] if output else None

        if guid:
            values = [(dicc["theme"], current_date, guid, dicc["user"]) for dicc in data]
            update_sql = 'UPDATE ZT_NEWSLETTER_USER_MASTER SET THEME = %s, UPDATED_ON = %s, GUID = %s WHERE USER = %s'
            cursor.executemany(update_sql, values)
        else:
            new_guid = str(uuid.uuid4().hex)
            values = [(dicc["user"], dicc["theme"], current_date, new_guid) for dicc in data]
            insert_sql = 'INSERT INTO ZT_NEWSLETTER_USER_MASTER (USER, THEME, UPDATED_ON, GUID) VALUES (%s, %s, %s, %s)'
            cursor.executemany(insert_sql, values)

        conn.commit()
    except Exception as e:
        handle_error(e)
    finally:
        cursor.close()
        conn.close()
    return jsonify({"upsert_user_master": "done"})



port = int(os.environ.get('PORT', 3000))
if __name__ == "__main__":
    app.run(host='0.0.0.0',port=port)
