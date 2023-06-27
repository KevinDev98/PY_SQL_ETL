# -*- coding: utf-8 -*-
"""
@author: kevin.gutierrez
"""
from IPython.display import display
import pandas as pd
import sys
from datetime import date
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

class CFunctions:
    pass

    sys.path.insert(0, 'C:\\Users\kevin.gutierrez\Documents\DEV\ETL_DUMY_FR\\')
    from CConectionSQL import CConectionSQL  # Nombre del archivo- Nombre de la clase
    from CCleanRules import CCleanRules
    from CControlAvisos import CControlAvisos

    def __init__(self):
        self.WhereActive = " And Active=1 "
        self.PathFilesProject = "C:\\Users\kevin.gutierrez\Documents\DEV\ETL_DUMY_FR\\Files\\"
        self.rules = self.CCleanRules()
        self.Dumy_ADLS_End = "C:\\Users\kevin.gutierrez\Documents\DEV\ETL_DUMY_FR\Files\ADLS DUMY\CONSUMPTIONS\\"
        #self.EndSynapse = "C:\\Users\kevin.gutierrez\Documents\DEV\ETL_DUMY_FR\Files\ADLS DUMY\\"

    ##-------------------------METODOS
    def connectsql(self, str_conn_sql):
        pass
        connection_url = URL.create("mssql+pyodbc", \
                                    query={"odbc_connect": str(str_conn_sql)})
        try:
            pass
            engine = create_engine(connection_url)
            # print("conexión a sql exitosa")
        except Exception as e:
            pass
            engine = "Error engine: {}".format(e)
        return engine

    def execquery(self, sql, query, where="", orderby="", groupby=""):
        pass
        try:
            pass
            engine = self.connectsql(sql)  # Crea connexion con SQL
            df_data = pd.DataFrame()
            querysql = str(query)  # Define query
            if len(str(where)) > 0:
                pass
                querysql = str(querysql) + " WHERE " + str(where)
            if len(str(orderby)) > 0:
                pass
                querysql = str(querysql) + " ORDER BY " + str(orderby)
            if len(str(groupby)) > 0:
                pass
                querysql = str(querysql) + " GROUP BY " + groupby

            # print("query SQL: "+querysql)
            with engine.begin() as conn:
                dataquery = pd.read_sql(querysql, conn)
                # print(dataquery.head())
                df_data = dataquery
        except Exception as e:
            pass
            print("error execquery: {}".format(e))
            df_data['Result'] = str(e)
        return df_data

    def GetScheduling(self, sql):  # Obtiene las calendarizaciones
        pass
        try:
            pass
            connection_url = URL.create("mssql+pyodbc", \
                                        query={"odbc_connect": str(sql)})
            engine = create_engine(connection_url)
            df_data = pd.DataFrame()
            if str(engine).__contains__("Error"):
                pass
                df_data['Result'] = str(engine)
            else:
                querysql = "SELECT PK_Scheduling,SchedulingName FROM SCHEDULING" \
                           + " WHERE GETDATE() BETWEEN StarDate AND EndDate " + self.WhereActive
                # print("query SQL: "+querysql)
                with engine.begin() as conn:
                    dataquery = pd.read_sql(querysql, conn)
                    # print(dataquery.head())
                    df_data = dataquery
        except Exception as e:
            pass
            print("error GetScheduling: {}".format(e))
        return df_data

    def getetl(self, sched, sql):  # obtiene los esquemas y tablas origen y destino
        pass
        try:
            pass
            connection_url = URL.create("mssql+pyodbc", \
                                        query={"odbc_connect": str(sql)})
            engine = create_engine(connection_url)
            querysql = "SELECT * FROM ETL" \
                       + " WHERE FK_Scheduling='" + str(sched) + "'" + self.WhereActive
            # print("query SQL: "+querysql)
            df_data = pd.DataFrame()
            with engine.begin() as conn:
                dataquery = pd.read_sql(querysql, conn)
                # print(dataquery.head())
                df_data = dataquery
        except Exception as e:
            pass
            df_data['Result'] = str(e)
            print("error getetl: {}".format(e))
        return df_data

    def GetFieldsPropierties(self, df, FKExtraciton, str_conn_sql):
        pass
        engine = self.connectsql(str_conn_sql)
        querysql = "SELECT Order_Field,Field, FieldType,FDateFormat, FK_Layer" \
                   + " FROM LAYOUT_FIELDS" \
                   + " WHERE FK_Layer='" \
                   + FKExtraciton + "'" + self.WhereActive
        try:
            pass
            with engine.begin() as conn:
                dataquery = pd.read_sql(querysql, conn)  # Extrae los campos
                # print(dataquery.head())
                Fields = dataquery
        except Exception as e:
            pass
            print("Error obteniendo propiedades campos: {}".format(e))
            Fields = df
        return Fields

    def GetFields(self, df, FKExtraciton, str_conn_sql):
        pass
        try:
            pass
            Fields = self.GetFieldsPropierties(df, FKExtraciton, str_conn_sql)
            list_fields = []
            for index, rows in Fields.iterrows():
                pass
                # print("campo: " + str(rows[1]))
                campo = str(rows[1])
                list_fields.append(campo.lower())  # se recomienda poner los headers en minusculas las minusculas

            Fields = df[list_fields]
            # print(Fields)
        except Exception as e:
            pass
            print("Error obteniendo campos: {}".format(e))
            Fields = df
        # print(Fields)
        return Fields

    def getemails(self, str_sql, sched):
        pass
        querysql = "SELECT Distinct EmailAddress FROM SENDINGEMAILS WHERE FK_Scheduling='" + str(sched) + "'" + self.WhereActive
        #print(querysql)
        df_emails= self.execquery(sql=str_sql, query=querysql)
        #display(df_emails)
        emailsto=""
        for idx, rowemail in df_emails.iterrows():
            pass
            #print(rowemail[0])
            emailsto=emailsto+str(rowemail[0])+";"
        #print(email)
        return emailsto
    def Extraction(self, Path, FName, delimiter, querysql="", Str_ConSQL=""):  # Crea y retorna un data frame
        FullPath = Path + FName
        # print(FullPath)
        try:
            if (delimiter == "sql"):  # Si la extracción va a ser de synapse, entonces debe recibir un query
                DF_Extraction = self.execquery(Str_ConSQL, querysql)
            else:
                if (FName.endswith("csv")):
                    """
                    DF_Extraction=spark.read\
                        .option('header','true')\
                            .option('inferSchema','true')\
                                .option("sep", str(delimiter))\
                                    .csv(FullPath)
                    """
                    DF_Extraction = pd.read_csv(FullPath, sep=delimiter)
                if (FName.endswith("parquet")):
                    # DF_Extraction=spark.read.parquet(FullPath)
                    DF_Extraction = pd.read_parquet(FullPath)
                if (FName.endswith("xlsx")):
                    DF_Extraction = pd.read_excel(FullPath)
                    # DF_Extraction=spark.createDataFrame(DF_info)
        except Exception as ex:
            print("Error creando dataframe: {}".format(ex))
        return DF_Extraction

    def Transformation(self, df):  # Simula reglas de negocio
        pass
        # Fields=self.GetFieldsPropierties(df,FKExtraciton,SQLConnection)
        try:
            pass
            for col in df.columns:
                pass
                # print("col: " + str(col))
                for index, rows in df.iterrows():
                    pass
                    # print(index)
                    # print(rows)
                    data = df.loc[index, col]
                    data = str(data)
                    # print("dato de entrada: " + data)
                    data = self.rules.Remove_Special_Characters(self.rules.Remove_spacewhite(data))
                    df.loc[index, col] = data
                    # print("dato de salida: " + data)

        except Exception as e:
            pass
            print("Error en transformación: {}".format(e))
        return df

    def Load(self, df_data, schemasql, tablename, str_conn_sql):
        pass
        try:
            pass
            #print(str_conn_sql)
            print(schemasql, tablename)
            #print(df_data)
            #df_data.style#pip install Jinja2
            engine_con = self.connectsql(str_conn_sql)
            with engine_con.begin() as conn:
                pass
                #name, con, schema = None, if_exists = 'fail', index = True, index_label = None, chunksize = None, dtype = None, method = None
                df_data.to_sql(name=tablename, con=conn, schema=schemasql, if_exists='append', index = False, method=None)
                return 1
        except Exception as e:
            pass
            print("Error En la carga de datos: {}".format(e))
            return 1

    def consumption(self, df_consumption, pathsave, filename, typefile, delimiterfile, fk_layer,sqlcon):
        pass
        engine = self.connectsql(sqlcon)
        querysql = "SELECT Field, ConsumptionField_Name" \
                   + " FROM LAYOUT_FIELDS" \
                   + " WHERE FK_Layer='" \
                   + fk_layer + "'" + self.WhereActive\
                   + ' And ConsumptionField=1 ORDER BY Order_Field asc'
        try:
            pass
            with engine.begin() as conn:
                dataquery = pd.read_sql(querysql, conn)  # Extrae los campos
                for index, row in dataquery.iterrows():
                    pass
                    print(row[1])
            df_consumption.to_csv(pathsave+filename+"."+typefile, sep=delimiterfile)
        except Exception as e:
            pass
            print("Error obteniendo propiedades campos: {}".format(e))

    def Execute_ETL(self, pd_df_scheduling, SQLConDBcontrol, SQLConSynSource='', SQLConSynEnd=''):
        pass
        try:
            pass
            print("Inicia ETL")
            SQLConDBcontrol = str(SQLConDBcontrol)
            # print(SQLConDBcontrol)
            index = 0
            # print(pd_df_scheduling.columns)
            for row in pd_df_scheduling.iterrows():
                pass
                PK_Layer = str(pd_df_scheduling.loc[index, "PK_Layer"])

                SourceSchema = str(pd_df_scheduling.loc[index, "SourceSchema"])
                SourceTable = str(pd_df_scheduling.loc[index, "SourceTable"])

                Active = int(pd_df_scheduling.loc[index, "Active"])
                # print(Active)

                EndSchema = str(pd_df_scheduling.loc[index, "EndSchema"])
                EndTable = str(pd_df_scheduling.loc[index, "EndTable"])

                FK_Scheduling = str(pd_df_scheduling.loc[index, "FK_Scheduling"])
                # print("calendarización: "+FK_Scheduling)
                # spark_df_scheduling=spark.createDataFrame(pd_df_scheduling)
                if (Active == 1):
                    pass
                    # spark_df_scheduling.show()
                    # read_file=Path_Start_Repository+Full_FileName_In
                    query = ""
                    DF_Extraction = self.Extraction(SourceSchema, SourceTable, ",", query, SQLConDBcontrol)
                    DF_Extraction = self.GetFields(DF_Extraction, PK_Layer, SQLConDBcontrol)
                    print("Extracción completa")

                    DF_Transformation = self.Transformation(DF_Extraction)  # Simula transformación
                    print("Transformación Completa")

                    consumption=self.Load(DF_Transformation, EndSchema, EndTable, SQLConSynEnd)

                    email_list=self.getemails(str_sql=SQLConDBcontrol, sched=FK_Scheduling)

                    now = str(date.today())
                    now = now.replace('-', '').replace(':', '').replace('.', '').replace(' ', '_')
                    dum_desc = {'Descripción': ["desc 1", "desc 2"], 'Donde sucedio': ["test 1", "test 2"], 'Fecha': [str(now), str(now)]}
                    df_log_dumy=pd.DataFrame(data=dum_desc)

                    if consumption == 1:
                        print("carga completa")
                        FK_TypeFile_Out = "csv"  # Dumy
                        Delimiter_Type_Out = "|"
                        self.consumption(df_consumption=DF_Transformation, pathsave=self.Dumy_ADLS_End, filename=EndTable, typefile=FK_TypeFile_Out, delimiterfile=Delimiter_Type_Out, fk_layer=PK_Layer, sqlcon=SQLConDBcontrol)
                        print("Consumo completo")
                        self.CControlAvisos.sendNotify(Subjectmail="success " + str(EndTable), EmailTo=email_list, Message="Carga correcta a " + EndTable, df_notify=df_log_dumy)
                    else:
                        pass
                        self.CControlAvisos.sendNotify(Subjectmail="bad " + str(EndTable), EmailTo=email_list, Message="Error cargando datos a " + EndTable, df_notify=df_log_dumy)
                    print("----------------------------------------")
                else:
                    print("Inactivo")
                #sys.exit()
                print("Termina calendarización: " + FK_Scheduling)
                index = index + 1
        except Exception as ex:
            pass
            print("error de en el proceso: {}".format(ex))
