
# TODO: PIP UNINSTALL PYODBC


from flask_api.dbconnection import EngineManager
manager = EngineManager()
engine = manager.get_engine('localdev')

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()




import json 
with open('C:/Users/Vitor Russomano/airflow/data/anbima/vna_2023-01-12.json','r') as _file:       
     data = json.load(_file)
 
VNASchema().load(data,many=True)
