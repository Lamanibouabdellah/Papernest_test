#Importing ############################
from sqlalchemy import create_engine
from pandas import read_sql_query, read_csv, to_datetime
from ftplib import FTP
import io, smtplib, csv, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

#Inputs ############################
#Database parameters :
db_host=''
db_user, db_password='', ''
db_dbname, db_port='', ''
##Database table :
table_name='clients_crm'
id, first_name, last_name, phone_number, creation_date = 'id', 'FirstName', 'LastName', 'PhoneNumber', 'CreationDate'
#FTP server parameters :
ftp_host, ftp_user, ftp_pass, ftp_port='', '', '',
#FTP csv :
base_path, file_name='files/', 'raw_calls.csv'
call_date, call_duration, call_number= 'date', 'duration_in_sec', 'incoming_number'
#email sending server:
smtp_server = 'smtp-relay.gmail.com'
smtp_port = 587
smtp_user = '------------'
smtp_password = '--------'
from_mail='from@example.com'
to_mail='to@example.com'

class client:
    def __init__ (self, ID, FIRST_NAME, LAST_NAME, PHONE_NUMBER, FIRST_CALL_DATE, AVG_CALL_DURATION):
        self.ID=ID
        self.FIRST_NAME=FIRST_NAME
        self.LAST_NAME=LAST_NAME
        self.PHONE_NUMBER=PHONE_NUMBER
        self.FIRST_CALL_DATE=FIRST_CALL_DATE
        self.AVG_CALL_DURATION=AVG_CALL_DURATION

class email:
    def __init__(self, from_, to_, subject, body, attach_clients):
        self.from_=from_
        self.to_=to_
        self.subject=subject
        self.body=body
        self.attach_clients=attach_clients

    def clients_to_csv (self): #make csv from attach_clients objects list
        header = ['ID', 'FIRST_NAME', 'LAST_NAME', 'PHONE_NUMBER', 'FIRST_CALL_TIME', 'AVG_CALL_DURATION']
        clients_to_csv=self.subject+'.csv'
        with open(clients_to_csv, 'w', encoding='UTF8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for _client in self.attach_clients :
                writer.writerow([_client.ID, _client.FIRST_NAME, _client.LAST_NAME, _client.PHONE_NUMBER, _client.FIRST_CALL_DATE, _client.AVG_CALL_DURATION])
        self.attach_clients=clients_to_csv

    def send (self) :
        mail = MIMEMultipart()#creat mail object
        self.body= MIMEText(self.body, 'plain')
        mail['Subject'] = self.subject
        mail['From'] = self.from_
        mail['To'] = self.to_
        mail.attach(self.body) #adding body
        with open(self.attach_clients, 'rb') as file:
            mail.attach(MIMEApplication(file.read(), Name=self.attach_clients))
        smtp_obj = smtplib.SMTP(smtp_server, smtp_port) # Create SMTP object
        smtp_obj.login(smtp_user, smtp_password)# Login to the server
        smtp_obj.sendmail(mail['From'], mail['To'], mail.as_string())# sending
        smtp_obj.quit()

if __name__ == '__main__':
    #Loading data ###########################
    #Database table loading :
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_dbname}')
    connection = engine.connect()
    query = f'SELECT * FROM {table_name} WHERE "{creation_date}"= (SELECT MAX("{creation_date}") FROM {table_name})'# SQL query to load data
    db_table_df = read_sql_query(sql=query, con=connection)
    #FTP CSV loading :
    ftp_server = FTP()
    ftp_server.connect(host=ftp_host, port=ftp_port)
    ftp_server.login(user=ftp_user, passwd=ftp_pass)
    ftp_server.cwd(base_path)
    loading = io.BytesIO()
    ftp_server.retrbinary('RETR ' + file_name, loading.write)
    contents = loading.read()
    loading.seek(0)
    ftp_table_df = read_csv(loading, engine='python')
    ftp_table_df[call_date] = to_datetime(ftp_table_df[call_date])#To be able to to compare call dates

    #email preparation:
    send_mail=False
    email_=email(from_=from_mail, to_=to_mail, subject=datetime.datetime.now().strftime("%Y-%m-%d_%HH%M"), body='', attach_clients=[])
    for client_db in db_table_df.iloc():
        client_df_ftp = ftp_table_df[ftp_table_df[call_number] == int(client_db[phone_number])] #select from ftp csv by phone number
        if not client_df_ftp.empty:#check if the client's phone number exist at least once :
            send_mail=True
            #generate client
            client_=client(ID=client_db[id], #From DB
                           FIRST_NAME=client_db[first_name], #From DB
                           LAST_NAME=client_db[last_name], #From DB
                           PHONE_NUMBER=client_db[phone_number], #From DB
                           FIRST_CALL_DATE=client_df_ftp[call_date].min(), #From FTP
                           AVG_CALL_DURATION=client_df_ftp[call_duration].mean()) #From FTP
            #fill mail body with clients' first and last names
            email_.body += client_.FIRST_NAME + ' ' + client_.LAST_NAME + '\n'
            #send client object to mail attachement
            email_.attach_clients.append(client_)

    #test :
    print(f'From : {email_.from_}')
    print(f'To : {email_.to_}')
    print(f'Subject : {email_.subject}')
    print(f'Body : "\n{email_.body}"')
    email_.clients_to_csv()
    #email_.send() #to send the mail, smtp config needs to be done for the sender

