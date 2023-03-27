from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# from . import extract,transform,load
import pandas as pd
import psycopg2
import psycopg2.extras
import numpy as np

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'extract_transform_load',
    default_args=default_args,
    description='ETL to datawarehouse ',
    schedule_interval=timedelta(hours=1),
)

def Extract():
    """
       function for extracting 
          data from a csv file
    """
    df=pd.read_csv("./immobilier_avito-final.csv")
    return df




def transform(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='Extract')
    df.drop(["frais_syndic","kind"],axis=1,inplace=True)

    ### fixing surface
    pattern = r"(\d+)m²|(\d+)m|(\d+)\smètre|(\d+)\sm²|(\d+)\sm"
    df_name=df["name"].str.extract(pattern)
    df_name['surface'] = df_name.apply(lambda x: x.dropna().iloc[0] if x.count() > 0 else np.nan, axis=1)
    df["surface"]=df_name["surface"]
    df_descriptions=df["descriptions"].str.extract(pattern)
    df_descriptions['surface'] = df_descriptions.apply(lambda x: x.dropna().iloc[0] if x.count() > 0 else np.nan, axis=1)
    df1=pd.DataFrame(df_name["surface"])
    df2=pd.DataFrame(df_descriptions["surface"])
    df_surface=pd.concat([df1,df2],axis=1)
    df_surface.columns=["surface_name","surface_descriptions"]
    df_surface['surface'] = df_surface.apply(lambda x: x.dropna().iloc[0] if x.count() > 0 else np.nan, axis=1)
    df["surface"]=df_surface["surface"]

    ### fixing etage
    regex = r"\d+\s*(?:étage|etage)|(?:étage|etage)\s*\d+\s*|\d+\s*(?:er|éme|ére)\s*(?:étage|etage)"
    df_etage=(df[df["etage"].isna()]["name"].apply(lambda x:re.findall(regex,x)[0] if len(re.findall(regex,x))>0 else np.nan)).dropna()
    df_etage=df_etage.apply(lambda x: re.findall(r'\d+',x)[0])
    df.loc[df_etage.index,"etage"]=df_etage

    ### subsetting dataframe
    df_new=df.dropna(subset=["etage","surface"])

    ### fixing salons
    df_maisons=df_new[df_new["type"]=='Maisons et villas']["Salons"].fillna("3.0")
    df_appartement=df_new[df_new["type"]=='Appartements']["Salons"].fillna("1.0")
    df_terr=df_new[df_new["type"]=='Terrains et fermes']["Salons"].fillna("0.0")
    df_Bure=df_new[df_new["type"]=='Bureaux et plateaux']["Salons"].fillna("0.0")
    df_maga=df_new[df_new["type"]=='Magasins, commerces et locaux industriels']["Salons"].fillna("0.0")
    df_new.loc[df_maisons.index,"Salons"]=df_maisons
    df_new.loc[df_appartement.index,"Salons"]=df_appartement
    df_new.loc[df_terr.index,"Salons"]=df_terr
    df_new.loc[df_Bure.index,"Salons"]=df_Bure
    df_new.loc[df_maga.index,"Salons"]=df_maga

    ### fixing age

    df_new["age"].fillna(df_new["age"].mode()[0],inplace=True)
    df_new.dropna(subset="Secteur",inplace=True)

    df_new.loc[df_new["price"]=="Prix non spécifié","price"]=np.nan
    df_new["price"]=df_new["price"].str.replace("\s|DH","").astype("float")
    df_new["surface"]=df_new["surface"].astype("float")
    df_new.dropna(subset=["price","surface"],inplace=True)
    regions=["Marrakech-Safi","Casablanca-Settat","Casablanca-Settat","Rabat-Salé-Kénitra","Rabat-Salé-Kénitra","Casablanca-Settat","Casablanca-Settat","Souss","Rabat-Salé-Kénitra","Fès-Meknès",
        "Rabat-Salé-Kénitra","Souss-Massa","Fès-Meknès","Casablanca-Settat","Casablanca-Settat","Tanger-Tétouan-Al Hoceïma","Tanger-Tétouan-Al Hoceïma","Casablanca-Settat",
         "Oriental","Casablanca-Settat","Rabat-Salé-Kénitra","Béni Mellal-Khénifra","Casablanca-Settat","Tanger-Tétouan-Al Hoceïma","Oriental","Tanger-Tétouan-Al Hoceïma","Casablanca-Settat","Casablanca-Settat","Casablanca-Settat","Casablanca-Settat","Oriental","Casablanca-Settat","Rabat-Salé-Kénitra","Sous-Massa", 
         "Rabat-Salé-Kénitra","Casablanca-Settat"]
    dico=dict(zip(df_new.location.unique(),regions))
    df_new["region"]="nan"
    for index,row in df_new.iterrows():
        df_new.loc[index,"region"]=dico[row["location"]]

    df_new["country"]="Morocco"
    df_new.drop("name",axis=1,inplace=True)
    df_new.drop_duplicates(inplace=True)

    return df_new

    



def load(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='transform')
    hostname = "localhost"
    database = "sell"
    username = "postgres"
    pwd = "password"
    port_id = 5432
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    ) 
    cursor = conn.cursor()

    ###filling Property table
    property_tab = df[["age","Salons","etage","descriptions"]].drop_duplicates()
    for index, row in property_tab.iterrows():
        cursor.execute("""
            INSERT INTO Property_Dim(age, salon, etage, descriptions) 
            VALUES (%s, %s, %s, %s)
        """, (
            row["age"],
            row["Salons"],
            row["etage"],
            row["descriptions"]
        ))

    conn.commit()

    ###filling Location table
    location_tab = df[["location","region","country","Secteur"]].drop_duplicates()
    for index, row in location_tab.iterrows():
        cursor.execute("""
            INSERT INTO Location_Dim(city, region, country, secteur) 
            VALUES (%s, %s, %s, %s)
        """, (
            row["location"],
            row["region"],
            row["country"],
            row["Secteur"]
        ))

    conn.commit()    

    ###filling Type table
    type_tab = df[["type"]].drop_duplicates()
    for index, row in type_tab.iterrows():
        cursor.execute("""
            INSERT INTO Type_Dim(typo) 
            VALUES (%s)
        """, (
            row["type"],
            
        ))

    conn.commit()

    ### filling Sales_table
    for index,row in df.iterrows():
        cursor.execute(""" 
            SELECT property_key FROM Property_Dim
            WHERE age = %s AND salon = %s AND etage = %s AND descriptions = %s
        """,(
            row["age"],
            row["Salons"],
            row["etage"],
            row["descriptions"]))
        
        property_key=cursor.fetchone()[0]

        cursor.execute(""" 
            SELECT location_key from Location_Dim
            WHERE city=%s and region=%s and country=%s and secteur=%s
        """,(
            row["location"],
            row["region"],
            row["country"],
            row["Secteur"]))
        location_key=cursor.fetchone()[0]

        cursor.execute(""" 
            SELECT type_key from Type_Dim
            WHERE typo=%s
        """,(
            row["type"],))
        type_key=cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO Sales_Fact(property_key,location_key,type_key, price,surface) 
            VALUES (%s, %s, %s, %s, %s)
        """,(
            property_key,
            location_key,
            type_key,
            row["price"],
            row["surface"]
        ))
        conn.commit()


    # Close the database connection
    cursor.close()
    conn.close()

     
        









### defining pipeline tasks

extract_task=PythonOperator(
        task_id='extract',
        python_callable=Extract,
        dag=dag,
)

transform_task=PythonOperator(
        task_id='transform',
        python_callable=transform,
        dag=dag,
        provide_context=True,
)

load_task=PythonOperator(
        task_id='load',
        python_callable=load,
        dag=dag,
        provide_context=True,
)

#defining pipeline

extract_task >> transform_task >> load_task