from datetime import datetime
from logging import exception
from pyspark.shell import spark
from pyspark.sql import *
from pyspark.sql.types import *
import pandas as pd

def transformUpper(data):
        try:
            print("transforming columns names into upper case")
            temp_var = data.copy()
            temp_var.columns = [col.upper()  for col in temp_var.columns]
            print("columns are transformed into uppercase")
            return temp_var
        except Exception as e:
            print("transfomation failed", e)
            return None

def removeAgeColumn(data):
    try:
        print("Removing age column")
        temp = data.copy()
        df_new = temp.drop('AGE', axis=1)
        print("Age column removed")
        return df_new
    except Exception as e:
        print("transformation Age column removal is failed", e)
        return None

def calculateAge(dfwithoutAge):
    try:
        current_date = pd.to_datetime(datetime.today())
        print("Calculating Age")
        dfwithoutAge['AGE'] = (current_date - dfwithoutAge['DATEOFBIRTH']).dt.days / 365.25
        dfwithoutAge['AGE'] = dfwithoutAge['AGE'].astype(int)
        return dfwithoutAge
    except Exception as e:
        print("Age is not able to calculate for dataframe", e)
        return None
    