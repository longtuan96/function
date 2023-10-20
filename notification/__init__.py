import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    conn = psycopg2.connect(dbname=techconfdb, user=longtuan03@postgresql-db-sv-tlt-proj3,password="Dragon03", host="postgresql-db-sv-tlt-proj3.postgres.database.azure.com")
    try:
        notification_query = cursor.execute("SELECT message, subject FROM notification WHERE id = {};".format(notification_id))

        cursor.execute("SELECT first_name, last_name, email FROM attendee;")
        attendees = cursor.fetchall()

        notification_completed_date = datetime.utcnow()

        notification_status = 'Notified {} attendees'.format(len(attendees))
        
        update_query = cursor.execute("UPDATE notification SET status = '{}', completed_date = '{}' WHERE id = {};".format(notification_status, notification_completed_date, notification_id))        

        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        conn.close()