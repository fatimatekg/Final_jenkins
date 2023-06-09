import os
import cx_Oracle
import configparser
import logging
from flask import Flask, request, jsonify
from flask_cors import cross_origin

# Set up logging
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename='D:/insert_log.log', level=logging.INFO, format=log_format)

app = Flask(__name__)

error_response = []
def get_connection(connection_name):
    logging.info(f"Getting connection for {connection_name}")
    config = configparser.ConfigParser()
    config.read('Database.ini')
    host = config[connection_name]['host']
    database = config[connection_name]['database']
    user = config[connection_name]['user']
    password = config[connection_name]['password']
    dsn = cx_Oracle.makedsn(host=host, port=1521, sid=database)
    logging.info(f"Connection parameters - host: {host}, database: {database}, user: {user}")
    connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    logging.info(f"Connection established for {connection_name}")
    return connection

def execute_queries(connection, queries, acc_id):
    cursor = connection.cursor()
    for query in queries:
        try:
            cursor.execute(query)
        except cx_Oracle.IntegrityError as e:
            error_message = f"IntegrityError: {str(e)}\nError executing query for account ID {acc_id}: {query}"
            logging.error(error_message)
            error_response.append(error_message)
            continue
        except Exception as e:
            error_message = f"Error: {str(e)}\nError executing query for account ID {acc_id}: {query}"
            logging.error(error_message)
            error_response.append(error_message)
            continue
    connection.commit()
    cursor.close()
    return error_response

def generate_insert_queries(account_ids, user_id, source_connection_name, target_connection_name):
    try:
        logging.info("Generating insert queries")

        # Establish the source and target connections
        source_connection = get_connection(source_connection_name)
        target_connection = get_connection(target_connection_name)

        # Read select queries from file
        logging.info("Reading select queries from file")
        with open(os.path.expanduser("~/Desktop/check.sql"), 'r') as file:
            select_queries = file.readlines()
            logging.info(f"Select queries read: {select_queries}")

        response = []

        # Build insert queries and delete queries for each select query and write them to files for each account id
        for acc_id in account_ids:
            logging.info(f"Generating insert queries for account ID: {acc_id}")
            account_check_query = "SELECT COUNT(*) FROM cisadm.ci_acct WHERE acct_id = :account_id"
            cursor = source_connection.cursor()
            cursor.execute(account_check_query, account_id=acc_id)
            count = cursor.fetchone()[0]
            cursor.close()

            if count == 0:
                logging.error(f"Account ID {acc_id} not found in the source database. Skipping to the next account ID.")
                response.append({
                    "account_id": acc_id,
                    "status": "Error",
                    "message": f"Account ID {acc_id} not found in the source database."
                })
                continue

            target_account_check_query = "SELECT COUNT(*) FROM cisadm.ci_acct WHERE acct_id = :account_id"
            cursor = target_connection.cursor()
            cursor.execute(target_account_check_query, account_id=acc_id)
            target_count = cursor.fetchone()[0]
            cursor.close()

            if target_count > 0:
                logging.warning(f"Account ID {acc_id} already present in the target database. Skipping to the next account ID.")
                response.append({
                    "account_id": acc_id,
                    "status": "Warning",
                    "message": f"Account ID {acc_id} already present in the target database."
                })
                continue

            insert_queries = []
            delete_queries = []

            for query in select_queries:
                query = query.replace(":account_id", f"'{acc_id}'")

                try:
                    cursor = source_connection.cursor()
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    column_names = [column[0] for column in cursor.description]
                    cursor.close()
                except Exception as e:
                    logging.error(f"Error executing select query: {query}")
                    logging.error(f"Error: {str(e)}")
                    response.append({
                        "account_id": acc_id,
                        "status": "Error",
                        "message": f"Error occurred (Select Query Execution Error: {str(e)}) for account ID: {acc_id}"
                    })
                    continue

                table_name = query.split()[3]

                for row in rows:
                    insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES "

                    values = []
                    for idx, value in enumerate(row):
                        if value is None:
                            values.append('Null')
                        elif isinstance(value, str):
                            value = value.replace("'", "''")
                            values.append(f"'{value}'")
                        elif isinstance(value, cx_Oracle.Date):
                            value_str = value.strftime("to_date('%d-%b-%y %H:%M:%S','DD-MON-RR HH24:MI:SS')")
                            values.append(value_str)
                        elif isinstance(value, cx_Oracle.LOB):
                            values.append(f"'{value.read()}'")
                        elif isinstance(value, cx_Oracle.Object) and value.type.schema == "MDSYS" and value.type.name == "SDO_GEOMETRY":
                            sdo_geometry_str = "MDSYS.SDO_GEOMETRY("
                            sdo_geometry_str += f"{value.SDO_GTYPE if value.SDO_GTYPE is not None else 'Null'},"
                            sdo_geometry_str += f"{value.SDO_SRID if value.SDO_SRID is not None else 'Null'},"
                            sdo_geometry_str += f"MDSYS.SDO_POINT_TYPE({value.SDO_POINT.X if value.SDO_POINT.X is not None else 'Null'}, {value.SDO_POINT.Y if value.SDO_POINT.Y is not None else 'Null'}, {value.SDO_POINT.Z if value.SDO_POINT.Z is not None else 'Null'}),"
                            sdo_geometry_str += f"{value.SDO_ELEM_INFO if value.SDO_ELEM_INFO is not None else 'Null'},"
                            sdo_geometry_str += f"{value.SDO_ORDINATES if value.SDO_ORDINATES is not None else 'Null'}"
                            sdo_geometry_str += ")"
                            values.append(sdo_geometry_str)
                        else:
                            values.append(str(value))
                        if column_names[idx] in ['USER_ID', 'FREEZE_USER_ID','COMPLETE_USER_ID']:
                            values[-1] = f"'{user_id}'"
                    insert_query += f"({', '.join(values)})"
                    insert_queries.append(insert_query)

                    delete_query = f"DELETE FROM {table_name} WHERE "
                    conditions = []
                    for idx, column_name in enumerate(column_names[:12]):
                        value = row[idx]
                        if value is None:
                            conditions.append(f"{column_name} IS NULL")
                        elif isinstance(value, str):
                            value = value.replace("'", "''")
                            conditions.append(f"{column_name} = '{value}'")
                        elif isinstance(value, cx_Oracle.Date):
                            value_str = value.strftime("to_date('%d-%b-%y %H:%M:%S','DD-MON-RR HH24:MI:SS')")
                            conditions.append(f"{column_name} = {value_str}")
                        elif isinstance(value, cx_Oracle.LOB):
                            values.append(f"'{value.read()}'")
                        else:
                            conditions.append(f"{column_name} = {value}")

                    delete_query += " AND ".join(conditions) + ";" + "commit;"
                    delete_queries.append(delete_query)

            with open(f'D:/Insert_queries_for_{acc_id}.sql', 'w') as file:
                file.writelines('\n'.join(insert_queries))

            response.append({
                "account_id": acc_id,
                "status": "Success",
                "message": "Insert queries generated successfully"
            })

            execute_queries(target_connection, insert_queries, acc_id)

            with open(f'D:/Delete_queries_for_{acc_id}.sql', 'w') as file:
                file.writelines('\n'.join(delete_queries))

            insert_count = len(insert_queries)
            delete_count = len(delete_queries)

            response.append({
                "account_id": acc_id,
                "status": "Success",
                "message": f"Insert queries executed successfully for account ID: {acc_id}"
            })
            response.append({
                "account_id": acc_id,
                "status": "Success",
                "message": f"Count of Insert queries for Account {acc_id}: {insert_count}"
            })
            response.append({
                "account_id": acc_id,
                "status": "Success",
                "message": f"Count of Roll Back queries for account {acc_id}: {delete_count}"
            })

        response.append({
            "status": "Success",
            "message": "All accounts processed successfully"
        })
        return response

    except cx_Oracle.DatabaseError as e:
        logging.error(f"An Oracle database error occurred: {str(e)}")
        response.append({
            "status": "Error",
            "message": f"Oracle database error: {str(e)}"
        })
        return jsonify(response)
    except FileNotFoundError as e:
        logging.error(f"File not found error occurred: {str(e)}")
        response.append({
            "status": "Error",
            "message": f"File not found error: {str(e)}"
        })
        return jsonify(response)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        response.append({
            "status": "Error",
            "message": f"Error: {str(e)}"
        })
        return jsonify(response)

# Main program


@app.route('/generate_insert_queries', methods=['POST'])
@cross_origin()
def generate_insert_queries_route():
    try:
        data = request.get_json()
        account_ids = data.get('account_ids', [])
        user_id = data.get('user_id', '').upper()
        source_connection_name = data.get('source_connection_name', '').casefold()
        target_connection_name = data.get('target_connection_name', '').casefold()

        response = generate_insert_queries(account_ids, user_id, source_connection_name, target_connection_name)
        # Check if any error messages were returned
        error_messages = [msg["message"] for msg in response if msg["status"] == "Error"]

        if len(error_messages) > 0:
            return jsonify({
                "status": "Error",
                "message": error_messages
            })
        else:
            return jsonify({
                "execution_status": error_response,
                "status": "Success",
                "message": response
            })
            
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return jsonify({
            "status": "Error",
            "message": f"Error: {str(e)}"
        })

if __name__ == '__main__':
    app.run(port=5000,debug=True)
    
    
