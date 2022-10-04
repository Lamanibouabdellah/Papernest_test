from sqlalchemy import create_engine
host='souscritootest.cuarmbocgsq7.eu-central-1.rds.amazonaws.com'
user='testread'
password='testread'
dbname='souscritootest'
port='5432'
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
connection = engine.connect()


table_name='clients_crm'
id, FirstName, LastName, PhoneNumber, CreationDate = 'id', 'FirstName', 'LastName', 'PhoneNumber', 'CreationDate'
result = connection.execute(f'SELECT {table_name}.{FirstName}, {LastName} FROM {table_name} WHERE {CreationDate}=MAX({CreationDate})')

'''from datetime import date
from sqlalchemy import create_engine, MetaData, Table, select, func
engine = create_engine('postgresql://testread:testread@souscritootest.cuarmbocgsq7.eu-central-1.rds.amazonaws.com:5432/souscritootest')
metadata = MetaData()
clients_crm = Table('clients_crm', metadata, autoload=True, autoload_with=engine)
#['id', 'FirstName', 'LastName', 'PhoneNumber', 'CreationDate']

lastday=date(2016, 1, 17)
#session.query(Ticker).order_by(desc('updated')).first()
query = select([clients_crm]).where(clients_crm.columns.CreationDate == func.max(clients_crm.columns.CreationDate))

connection = engine.connect()
ResultProxy = connection.execute(query)
ResultSet = ResultProxy.fetchall()
print(ResultSet)'''


'''from ftplib import FTP
ftp_server = FTP()
ftp_server.connect('35.157.119.136', 21)
ftp_server.login('candidate','XHf8CAwZFzTtfK7qxZ')
ftp_server.cwd('files/')

print(ftp_server.dir())'''

