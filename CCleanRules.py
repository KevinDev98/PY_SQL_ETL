# -*- coding: utf-8 -*-
"""
@author: kevin.gutierrez
"""

class CCleanRules():
    
    def __init__(self):
        self.errordata=[]
        self.desc_errores=[]
        
    def Remove_spacewhite(self,s): #this method receives a string for spaces white remove
        try: 
            s=str(s) #Transform to string so that value can be read
            s=s.lstrip().rstrip().strip()#remove white spaces of string
        except Exception as ex:
            s='Error' #Error is returned
            print('Error Cleanning: {}' .format(ex))
        return s

    def Remove_Special_Characters(self,s):#this method receives a string for special characteres remove
        s=str(s)#Transform to string so that value can be read
        #s=s.replace(" ","")    
        CaracteresEspeciales=["#",'$','%','&','!','|','[',']','{','}','/','_',';',':',',','*',"'",'`'] #Special Characteres List   
        try:
            for z in s: #loop through each character of recived word
                if(z in CaracteresEspeciales):  #Validate if the current character is in the Special Characteres list
                    try:                    
                        s=s.replace(z,'')#the special character is removed
                    except Exception as ex:                
                        print('error replace: {}'.format(ex))
            return s
        except Exception as ex:
            print('error replace 0: {}'.format(ex)) 
