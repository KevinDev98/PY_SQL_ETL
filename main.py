import sys

sys.path.insert(0, 'C:\\Users\kevin.gutierrez\Documents\DEV\ETL_DUMY_FR\\')
from CConectionSQL import CConectionSQL  # Nombre del archivo- Nombre de la clase
# from CConnectionSpark import CConnectionSpark
from CFunctions import CFunctions

##----------INSTANCIA DE CLASES
RSetSQL = CConectionSQL()  # Instancia de clase SQL
Functs = CFunctions()

##-----------------VARIABLES GLOBALES
WhereActive = " And Active=1 "
PathFilesProject = "C:\\Users\kevin.gutierrez\Documents\DEV\ETL_DUMY_FR\\Files\\"
SQLConnection = RSetSQL.Str_Connect_BD_Control(PathFilesProject)
SQLConSynapseEnd=RSetSQL.Str_Connect_BD_End_Synapse(PathFilesProject)

if __name__ == '__main__':
    try:
        pass
        print("Inicia proceso")
        # spark=CConnectionSpark().Spark_Session() #Inicia Spark

        pd_Scheduling = Functs.GetScheduling(SQLConnection)
        # print(pd_Scheduling)

        for col in pd_Scheduling.columns:
            pass
            # print("col: "+str(col))
            break

        for index, row in pd_Scheduling.iterrows():
            pass
            # print("value: "+str(row[0]))
            z = str(row[0])
            print("calendarización: " + z)
            pd_df_ETL = Functs.getetl(z, SQLConnection)
            # print(pd_df_ETL
            Functs.Execute_ETL(pd_df_ETL, SQLConnection,SQLConSynEnd=SQLConSynapseEnd)
            # break
        # sys.exit()
    except Exception as ex:
        pass
        print("error ejecutando ETL: {}".format(ex))

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
