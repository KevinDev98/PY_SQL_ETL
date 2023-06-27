# -*- coding: utf-8 -*-
"""
@author: kevin.gutierrez
"""
import requests
import sys

class CControlAvisos():
    def sendNotify(Subjectmail,EmailTo,Message, df_notify):
        try:        
            """
            El proceso de carga de información fue ejecutado con éxito. <br>
            La carga de datos de las tablas de personas cargo a las 6:00 am del día 20-08-2021 <br>
            """
            rows=""
            for idx, msg in df_notify.iterrows():
                rows=rows+ "<tr>" \
                     + "<td>" + str(msg[0]) + "</td>" \
                     + "<td>" + str(msg[1]) + "</td>" \
                     + "<td>" + str(msg[2]) + "</td>" \
                     + "</tr>"


            url="https://prod-48.eastus2.logic.azure.com:443/workflows/b6e0490aa90b43b08f4b33136312eddc/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=DgycYHofH0HODsdYoGmPbjR21LnJGJcDqkv4aIYtnnM"
            json_request = {"ADF_Name":Subjectmail,"EmailTo":EmailTo,
            "Body":"""<html>
            <head>
            <style>
            #Data_Table {
            font-family: Arial, Helvetica, sans-serif;
            border-collapse: collapse;
            width: 100%;
            }
        
            #Data_Table td, #Data_Table th {
            border: 1px solid #ddd;
            padding: 8px;
            }
        
            #Data_Table tr:nth-child(even){background-color: #f2f2f2;}
        
            #Data_Table tr:hover {background-color: #ddd;}
        
            #Data_Table th {
            padding-top: 12px;
            padding-bottom: 12px;
            text-align: left;
            background-color: #04AA6D;
            color: white;
            }
            #Mensaje
            {
            background: #064;
            color: white;
            }
            </style>
            </head>
            <body>
        
            <table id='Data_Table'>
            <tr>
                <th>Descripción</th>
                <th>Donde Sucedio</th>
                <th>Fecha</th>
            </tr>"""

            + rows +
        
            """</table>
        
            <div id='Mensaje'>""" +
            Message
            + """</div>
            </body>
            </html>""","Error":""}

            #print(json_request)
            #requests.post(url, json=json_request)
            print("correo enviado correctamente")
            #response = sendrequests.json()
        except Exception as ex:        
            print('Error enviando correo:' + str(ex))    

