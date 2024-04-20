# importar librerias
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text as sql_text
import psycopg2
from email import message
import smtplib

# obtener la fecha de hoy
fecha_hoy = datetime.now().strftime('%Y-%m-%d')


# credenciales API de la nasa
api_url = 'https://api.nasa.gov/neo/rest/v1/feed?'
api_key = "1vQKAD9leMbOeppmESs3aFLlbjnAcoylVNyxdWdj"


def consulta_api_nasa():
    print("Hola")
    # parametros
    parametros = {
        'start_date': fecha_hoy,
        'end_date': fecha_hoy,
        'api_key': api_key
        }
    # consulta a la API mediante GET
    respuesta_cruda = requests.get(api_url, params = parametros)
    if respuesta_cruda.status_code == 200:
        data = respuesta_cruda.json()
    else:
        print("No se pudo consultar a la API")
    return data
    

# funcion para pasar a dataset
def convertir_dataframe():
    data = consulta_api_nasa() 

    # Acceder a los resultados
    objetos = data.get('near_earth_objects', {}).get(fecha_hoy, {})

    # verificar si el json contiene datos
    if not objetos:
        print("No hay registros para esta fecha")
    else:
        # crear dataframe
        neo = pd.DataFrame(objetos)

        # convertir id a tipo deseado
        neo['neo_reference_id'] = neo['neo_reference_id'].astype(int)

        # crear columnas de interes
        neo['estimated_diameter_min'] = np.nan
        neo['estimated_diameter_max'] = np.nan
        neo['close_approach_date_full'] = np.nan
        neo['kilometers_per_second'] = np.nan
        neo['distance_kilometers'] = np.nan
        neo['orbiting_body'] = np.nan

        # insertar los datos en la tabla de interes
        for index, row in neo.iterrows():

            id = row['neo_reference_id']
            neo.loc[neo['neo_reference_id'] == id, 'estimated_diameter_min'] = float(neo.loc[neo['neo_reference_id'] == id]['estimated_diameter'].iloc[0]['meters']['estimated_diameter_min'])
            neo.loc[neo['neo_reference_id'] == id, 'estimated_diameter_max'] = float(neo.loc[neo['neo_reference_id'] == id]['estimated_diameter'].iloc[0]['meters']['estimated_diameter_max'])
            neo.loc[neo['neo_reference_id'] == id, 'close_approach_date_full'] = neo.loc[neo['neo_reference_id'] == id]['close_approach_data'].iloc[0][0]['close_approach_date_full']
            neo.loc[neo['neo_reference_id'] == id, 'kilometers_per_second'] = float(neo.loc[neo['neo_reference_id'] == id]['close_approach_data'].iloc[0][0]['relative_velocity']['kilometers_per_second'])
            neo.loc[neo['neo_reference_id'] == id, 'distance_kilometers'] = float(neo.loc[neo['neo_reference_id'] == id]['close_approach_data'].iloc[0][0]['miss_distance']['kilometers'])
            neo.loc[neo['neo_reference_id'] == id, 'orbiting_body'] = neo.loc[neo['neo_reference_id'] == id]['close_approach_data'].iloc[0][0]['orbiting_body']

        # eliminar columnas no deseadas
        columnas_a_eliminar = ['links', 'nasa_jpl_url', 'id', 'estimated_diameter', 'close_approach_data']
        for i in columnas_a_eliminar:
            neo = neo.drop(i, axis=1)
        neo_json = neo.to_dict()
        print(f'Cantidad de registros extraidos el {fecha_hoy} - {neo.shape[0]}')
        return neo_json


def cargar_redshift():
    neo_json = convertir_dataframe()
    neo = pd.DataFrame(neo_json)
    print(f'Se van a insertar {neo.shape[0]} registros.')
    try:
        # proceso para conectar. INSERTAR CREDENCIALES!!
        engine = create_engine("postgresql://lucaforziati_coderhouse:H2wt20L9MF@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database")
        # cargar dataset a redshift
        neo.to_sql('neo', con = engine, index=False, if_exists='append')
        print("Conectado correctamente")
        engine.dispose()
    except Exception as e:
        print("No fue posible conectar")
        print(e)

def notificar_neo():

    # Obtener aquellos objetos que pasen cerca de la tierra y puedan ser una amenaza.
    # El criterio será establecido de forma aleatoria. En este caso se toma aquellos que pasen mas cerca de 5.000.000 km
    # o que "is_potentially_hazardous_asteroid" sea True. 

    def id_objetos(dataset):
        ids = ''
        for i in dataset['neo_reference_id']:
            ids += f'\n      - Neo_reference_id: {i}.'
        return ids

    neo_json = convertir_dataframe()
    neo = pd.DataFrame(neo_json)
    neo_filtrado = neo.loc[(neo['distance_kilometers'] < 5000000) | (neo['is_potentially_hazardous_asteroid'] == True)]
    
    if neo_filtrado.empty:
        print("No hay registros")
        None
    else:
        print("Hay registros")
        body_text_notificacion = f'''Estimado/a Cientifico/a: 
            El escaner de fecha {fecha_hoy} ha detectado {neo_filtrado.shape[0]} objetos de potencial riesgo. 
            IDs de los objetos: {id_objetos(neo_filtrado)} \n
            Esperamos respuesta acuerdo al protocolo.'''

        # conexion para enviar el mail
        try:
            x=smtplib.SMTP('smtp-mail.outlook.com',587)
            x.starttls()
            # datos del remitente y destintario
            remitente = 'juanmaranello@hotmail.com'
            contrasenia = 'philco123'
            destinatario = 'juanmaranello@hotmail.com'
            # conexion al mail remitente
            x.login(remitente, contrasenia)
            # Motivo del mail
            subject='Se han detectado objetos cercanos a la tierra'
            # cuerpo del mail
            body_text= body_text_notificacion
            # Cuerpo del mail
            message='Subject: {}\n\n{}'.format(subject,body_text)
            # enviar
            x.sendmail(remitente, destinatario ,message)
            print('Exito')
        except Exception as exception:
            print(exception)
            print('Failure')    

