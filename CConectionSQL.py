# -*- coding: utf-8 -*-
"""
@author: kevin.gutierrez
"""
import pyodbc
import pandas as pd
import os
import sys

class CConectionSQL:      
               
    def GetFileName(self, Path, patternFname):
        try:                
            filenameC=[patternFname]#Se define el nombre del archivo de config en una lista
            GetSettingsFiles=[
                CSQL for CSQL in sorted(os.listdir(Path))
                    if any(CSQL.startswith(file) for file in filenameC)
                ] #Se busca el archivo en el path definido
            #print(GetSettingsFiles)
            if (len(GetSettingsFiles)==0): #Se valida si se encontro el archivo
                ReadFile="File not Found"
            else:        
                ReadFile=GetSettingsFiles[0]
            #print(ReadFile)
        except Exception as ex:
            ReadFile="File not Found"
            print("Ocurrio un error en el proceso {}".format(ex))
        #print(ReadFile)
        return ReadFile  

    def Connect_BD_Control(self, PathF):
        #Path donde se encontrará el archivo de configuración para conectar a SQL
        strFileName=self.GetFileName(PathF,"ConexionSQL-BDC - V2") #Obtiene el nombre del archivo
        try:
            if(strFileName=="File not Found"): #Valida si el archivo a sido encontrado                
                print (strFileName)
                sys.exit()
            else:
                strFileName=PathF+strFileName
        except Exception as ex:
            print("Ocurrio un error en el proceso {}".format(ex))
        ReadFile=pd.read_excel(strFileName, sheet_name="ConBDControl")
        #print(ReadFile.columns)
        #Leeara los valores para la conexión
        Conf=ReadFile["SecretValue"].values
        srvr=str(Conf[0])
        usr=str(Conf[1])
        pwd=str(Conf[2])
        db=str(Conf[3])
        
        try:    
            StrConnect='DRIVER={ODBC Driver 17 for SQL Server};SERVER='+str(srvr).strip() \
                +';DATABASE='+str(db).strip() \
                    +';UID='+str(usr).strip() +';PWD='+str(pwd).strip()
            #print(StrConnect)
            #Connection=StrConnect
            Connection=pyodbc.connect(StrConnect)
            print('Conexion establecida con SQL SERVER')
        except Exception as ex:
            print('Error Connection: {}' .format(ex))
            Connection=ex
        return Connection          
    
    def Str_Connect_BD_Control(self, PathF):
        #Path donde se encontrará el archivo de configuración para conectar a SQL
        strFileName=self.GetFileName(PathF,"ConexionSQL-BDC - V2") #Obtiene el nombre del archivo
        try:
            if(strFileName=="File not Found"): #Valida si el archivo a sido encontrado                
                print (strFileName)
                sys.exit()
            else:
                strFileName=PathF+strFileName
        except Exception as ex:
            print("Ocurrio un error en el proceso {}".format(ex))
        ReadFile=pd.read_excel(strFileName, sheet_name="ConBDControl")
        #print(ReadFile.columns)
        #Leeara los valores para la conexión
        Conf=ReadFile["SecretValue"].values
        srvr=str(Conf[0])
        usr=str(Conf[1])
        pwd=str(Conf[2])
        db=str(Conf[3])
        
        try:    
            StrConnect='DRIVER={ODBC Driver 17 for SQL Server};SERVER='+str(srvr).strip() \
                +';DATABASE='+str(db).strip() \
                    +';UID='+str(usr).strip() +';PWD='+str(pwd).strip()            
        except Exception as ex:
            print('Error Connection: {}' .format(ex))
            StrConnect=ex
        return StrConnect 
    
    def Str_Connect_BD_End_Synapse(self, PathF):
        #Path donde se encontrará el archivo de configuración para conectar a SQL
        strFileName=self.GetFileName(PathF,"EndSynapse") #Obtiene el nombre del archivo
        try:
            if(strFileName=="File not Found"): #Valida si el archivo a sido encontrado                
                print (strFileName)
                sys.exit()
            else:
                strFileName=PathF+strFileName
        except Exception as ex:
            print("Ocurrio un error en el proceso {}".format(ex))
        ReadFile=pd.read_excel(strFileName, sheet_name="EndSynapse")
        #print(ReadFile.columns)
        #Leeara los valores para la conexión
        Conf=ReadFile["SecretValue"].values
        srvr=str(Conf[0])
        usr=str(Conf[1])
        pwd=str(Conf[2])
        db=str(Conf[3])
        
        try:    
            StrConnect='DRIVER={ODBC Driver 17 for SQL Server};SERVER='+str(srvr).strip() \
                +';DATABASE='+str(db).strip() \
                    +';UID='+str(usr).strip() +';PWD='+str(pwd).strip()            
        except Exception as ex:
            print('Error Connection: {}' .format(ex))
            StrConnect=ex
        return StrConnect
        
        
    
        
        
        
        
        
                  