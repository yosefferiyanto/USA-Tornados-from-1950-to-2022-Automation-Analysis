'''
=================================================
Milestone 3 Phase 2

Nama  : Yosef Feriyanto
Batch : FTDS-007-HCK

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL, Data Cleaning, Migrasi ke ElasticSearch, & Great Expectation.
Adapun dataset yang dipakai adalah dataset mengenai bencana tornado di AS dari tahun 1950 s.d. 2022.
=================================================
'''

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

# Menjalin koneksi postgres dengan python

def connect_to_postgres():
    '''
  Fungsi ini ditujukan untuk menyambungkan koneksi postgres dengan python.

  Parameters:
   user: string - user yang digunakan
   passwordd: string - password user di atas
   host: string - host adress yang digunakan
   port: string - port yang digunakan
   database: string - nama data base yang diminta yaitu "db_phase2"

  Return
   Connected or Error
  '''
    try:
        connection = db.connect(
            user="postgres",
            password="y053ffy4nt0",
            host="127.0.0.1",
            port="5432",
            database="db_phase2"
        )
        cursor = connection.cursor()
        print("-------Connected to the database!-------")
        return connection, cursor
    except Exception as error:
        print("-------Error while connecting to PostgreSQL:", error)
        return None, None


# Memasukkan data dalam PostgreSQL

def copy_csv_to_table(cursor, connection, file_path=r"C:/Windows/Temp/Private/P2G7_Yosef_Feriyanto_Data_Raw.csv"):
    '''
  Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.

  Parameters:
   file_path: string - lokasi lokal data raw P2G7_Yosef_Feriyanto_Data_Raw.csv yang digunakan
   table: string - nama table yang telah dibuat dalam database "db_phase2"
   sql_query: string - query untuk memasukkan data dalam database dan table yang digunakan

  Return
   None or Error
  '''
    try:
        # Menyiapkan perintah SQL COPY
        sql = """
        COPY table_gc7("om", "yr", "mo", "dy", "date", "time", "tz", "datetime_utc", "st",
               "stf", "mag", "inj", "fat", "loss", "slat", "slon", "elat", "elon",
               "len", "wid", "ns", "sn", "f1", "f2", "f3", "f4", "fc")
        FROM STDIN WITH CSV HEADER DELIMITER ',' NULL 'NA';
        """

        # Membuka file CSV dan menjalankan perintah COPY
        with open(file_path) as f:
            cursor.copy_expert(sql, f)

        # Commit transaksi
        connection.commit()
        
        print("Data successfully copied to the table!")
    except Exception as error:
        print("Error while copying data to PostgreSQL table:", error)
        print(f"File Path: {file_path}")
        print(f"SQL Command: {sql}")
'''
Khusus fungsi ini tidak berjalan, maka saya melakukan input data ke postgresql menggunakan syntax ini dalam notebook lain:

  # Menyiapkan perintah SQL COPY
  sql = """
  COPY table_gc7("om", "yr", "mo", "dy", "date", "time", "tz", "datetime_utc", "st",
        "stf", "mag", "inj", "fat", "loss", "slat", "slon", "elat", "elon",
        "len", "wid", "ns", "sn", "f1", "f2", "f3", "f4", "fc")
  FROM STDIN WITH CSV HEADER DELIMITER ',' NULL 'NA';
  """

  # Membuka file CSV dan menjalankan perintah COPY
  with open(r"C:\Windows\Temp\Private\P2G7_Yosef_Feriyanto_Data_Raw.csv") as f:
      cursor.copy_expert(sql, f)

  # Commit transaksi
  connection.commit()
  '''

# Mengambil data dari PostgreSQL dan langsung masuk dalam dataframe "df"

def fetch_data_from_table(cursor, connection, table_name="table_gc7"):
    '''
  Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.

  Parameters:
   query: string - berisi query untuk mengambil semua data yang ada dalam table
   table: string - nama table dimana data disimpan, yaitu "table_gc7"

  Return
   data: list of str - daftar data yang ada di database
     
  Contoh penggunaan:
  df = fetch_data_from_table(cursor, connection, table_name='table_gc7')
  '''
    try:
        # Query untuk mengambil semua data dari tabel
        query = f"SELECT * FROM {table_name};"
        cursor.execute(query)

        # Mengambil semua baris hasil query
        rows = cursor.fetchall()

        # Mengubah data ke dalam DataFrame
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

        return df
    except Exception as error:
        print("Error while fetching data from PostgreSQL table:", error)
    finally:
        
        # Menutup koneksi
        cursor.close()
        connection.close()


# Analisa dan handle missing value dalam kolom "loss" atau kerugian akibat tornado

def handle_missing_loss_values(df):
    '''
  Fungsi ini ditujukan untuk data cleaning missing value dengan memberikan hasil analisa berikut keputusan handlingnya.

  Parameters:
   database: string - nama database dimana data disimpan, yaitu "df"

  Return
   data: list of str - daftar hasil analisa dan perbedaan sebelum dan setelah handling
     
  Contoh penggunaan:
  df = handle_missing_loss_values(df)
  '''
    # Cek missing value
    missing_values = df.isnull().sum()
    print("Missing values before imputation:\n", missing_values)
    
    # Investigasi missing value pada kolom 'loss'
    loss_nan_counts_per_year = df[df['loss'].isna()].groupby('mag').size()
    print("\nMissing values in 'loss' column by 'mag':\n", loss_nan_counts_per_year)
    print("\nSetelah saya groupby data NaN dalam kolom 'loss' terhadap 'mag' untuk menganalisa")
    print("\nDapat dilihat nilainya menurun dari magnitud 0 s.d. 5; jumlah NaN menurun dari 19877 s.d. 6")
    print("\nOleh karena itu, dapat diambil kesimpulan bahwa data NaN dalam kolom 'loss' ini adalah 0 atau tanpa kerugian")
    
    # Imputasi missing value pada kolom 'loss' dengan angka 0
    df['loss'] = df['loss'].fillna(0)
    
    # Cek kembali missing value setelah imputasi
    missing_values_after = df.isnull().sum()
    print("\nMissing values after imputation:\n", missing_values_after)
    
    return df


# Analisa dan handle missing value dalam kolom "mag" atau skala magnitud tornado

def handle_missing_mag_values(df):
    '''
  Fungsi ini ditujukan untuk data cleaning missing value dengan memberikan hasil analisa berikut keputusan handlingnya.

  Parameters:
   database: string - nama database dimana data disimpan, yaitu "df"

  Return
   data: list of str - daftar hasil analisa dan perbedaan sebelum dan setelah handling
     
  Contoh penggunaan:
  df = handle_missing_mag_values(df)
  '''
    # Cek missing value
    missing_values = df.isnull().sum()
    print("Missing values before imputation:\n", missing_values)
    
    # Investigasi missing value pada kolom 'mag'
    mag_nan_counts_per_loss = df[df['mag'].isna()].groupby('loss').size()
    print("\nMissing values in 'mag' column by 'loss':\n", mag_nan_counts_per_loss)
    print("\nSetelah saya groupby data NaN dalam kolom 'mag' terhadap 'loss' untuk menganalisa")
    print("\nDapat dilihat data NaN dalam kolom 'mag' hampir semuanya dalam loss = 0 atau tanpa kerugian")
    print("\nOleh karena itu, dapat diambil kesimpulan bahwa data NaN dalam kolom 'mag' ini adalah 0 atau magnitud terendah")
    
    # Imputasi missing value pada kolom 'mag' dengan angka 0
    df['mag'] = df['mag'].fillna(0)
    
    # Cek kembali missing value setelah imputasi
    missing_values_after = df.isnull().sum()
    print("\nMissing values after imputation:\n", missing_values_after)
    
    return df


# Analisa dan handle data duplikat

def handle_duplicated_data(df):
    '''
  Fungsi ini ditujukan untuk data cleaning data duplikat dengan memberikan hasil analisa data duplikat apakah banyak atau tidak,
  berikut keputusan handlingnya.

  Parameters:
   database: string - nama database dimana data disimpan, yaitu 'df'

  Return
   data: list of str - daftar hasil analisa dan perbedaan sebelum dan setelah handling
     
  Contoh penggunaan:
  df = handle_duplicated_data(df)
  '''
    # Cek data duplikat sebelum handling
    duplicated_check = df.duplicated().sum()
    print("Duplicated Data Count:\n",duplicated_check)

    # Menampilkan data duplikat
    print(df[df.duplicated(keep=False)])

    # Handling data duplikat yaitu drop, karena hanya satu
    df.drop_duplicates(inplace=True)

    # Cek data duplikat setelah handling
    duplicated_check_after = df.duplicated().sum()
    print("\nDuplicated Data After Drop:\n",duplicated_check_after)

    return df


# Time process untuk membuat kolom 'tornado_duration'

def time_process(df):
    '''
  Fungsi ini ditujukan untuk membuat kolom 'end_time' dari kolom 'datetime_utc', dan membuat kolom 'start_time' dari kolom 'date' & 'time',
  lalu membuat kolom 'tornado_duration' dari hasil pengurangan 'end_time' dengan 'start_time', juga mengubah tipe datanya menjadi date. 

  Parameters:
   database: string - nama database dimana data disimpan, yaitu 'df'

  Return
   None
     
  Contoh penggunaan:
  df = time_process(df)
  '''
    # Ubah tipe data dari kolom 'datetime_utc' menjadi datetime
    df['end_time'] = pd.to_datetime(df['datetime_utc']).dt.tz_localize(None)  # Menghapus informasi zona waktu

    # Hapus kolom 'datetime_utc'
    df.drop('datetime_utc', axis=1, inplace=True)

    # Gabungkan kolom 'date' dan 'time' untuk membuat kolom 'start_time'
    df['start_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])

    # Hapus kolom 'date' & 'time'
    df.drop(['date', 'time'], axis=1, inplace=True)

    # Hitung durasi tornado
    df['tornado_duration'] = df['end_time'] - df['start_time']
    
    return df


# Location Process

def location_process(df):
    '''
    Fungsi untuk memproses DataFrame:
    - Membuat kolom 'start_location' dari 'slon' dan 'slat'
    - Membuat kolom 'end_location' dari 'elon' dan 'elat'
    - Menghapus kolom 'slat', 'slon', 'elat', 'elon'

    Parameters:
    database: string - nama database dimana data disimpan, yaitu 'df'

    Return
    None

    Contoh penggunaan:
    df = location_process(df)
    '''
    # Membuat kolom 'start_location' dan 'end_location'
    # Format untuk geo_point di Elasticsearch: "lat,lon"
    df['start_location'] = df.apply(lambda row: f"{row['slat']},{row['slon']}", axis=1)
    df['end_location'] = df.apply(lambda row: f"{row['elat']},{row['elon']}", axis=1)

    return df



# Rename & mengubah tipe data kolom

def rename_and_transform_columns(df):
    '''
  Fungsi ini ditujukan untuk mengubah nama-nama kolom
  & mengubah tipe data kolom 'mag'/'magnitude' menjadi integer karena nilainya kategorikal 1 s.d. 5. 

  Parameters:
   database: string - nama database dimana data disimpan, yaitu 'df'

  Return
   None
     
  Contoh penggunaan:
  df = rename_and_transform_columns(df)
  '''    
    # Rename columns
    column_mapping = {
        'om': 'tornado_id',
        'yr': 'year',
        'mo': 'month',
        'dy': 'day',
        'tz': 'time_zone',
        'st': 'state',
        'stf': 'state_fips',
        'mag': 'magnitude',
        'inj': 'injuries',
        'fat': 'fatalities',
        'slat': 'start_lat',
        'slon': 'start_lon',
        'elat': 'end_lat',
        'elon': 'end_lon',
        'len': 'length',
        'wid': 'width',
        'ns': 'state_affect',
        'sn': 'state_affect_bin',
        'f1': 'first_cnty_affect',
        'f2': 'second_cnty_affect',
        'f3': 'third_cnty_affect',
        'f4': 'fourth_cnty_affect',
        'fc': 'was_mag_est'
    }
    df.rename(columns=column_mapping, inplace=True)

    # Convert 'mag'/'magnitude' column to integer type
    df['magnitude'] = df['magnitude'].astype(int)

    return df

# Memanggil/menggunakan semua fungsi dalam script ini

def main_process():
    '''
  Fungsi ini ditujukan untuk menggunakan semua fungsi dalam script ini,
  menyimpan data yang telah dibersihkan sebagai "P2G7_Yosef_Feriyanto_Data_Clean.csv",
  sekaligus menginisiasi datanya dalam Elastic Search.

  Parameters:
   database: string - nama database dimana data disimpan, yaitu 'df'
   table: string - nama table dimana data disimpan, 'yaitu table_gc7'

  Return
   Data Saved atau Failed to Connect
     
  Contoh penggunaan:
  main_process()
  '''
    connection, cursor = connect_to_postgres()
    if connection and cursor:
        df = connect_to_postgres(df)
        df = copy_csv_to_table(df)
        df = fetch_data_from_table(cursor, connection)
        df = handle_missing_loss_values(df)
        df = handle_missing_mag_values(df)
        df = handle_duplicated_data(df)
        df = time_process(df)
        df = location_process(df)
        df = rename_and_transform_columns(df)
        
        # Menyimpan DataFrame ke file CSV
        df.to_csv('P2G7_Yosef_Feriyanto_Data_Clean.csv', index=False)
        print("-------Data Saved------")

        # Initialize Elasticsearch
        es = Elasticsearch(hosts=["http://localhost:9200"])
        
        # Read the cleaned data
        df = pd.read_csv('P2G7_Yosef_Feriyanto_Data_Clean.csv')
        
        # Index data into Elasticsearch
        for i, r in df.iterrows():
            doc = r.to_json()
            res = es.index(index="table_m3", id=i+1, body=doc)
            print(res)
    else:
        print("Failed to connect to the database.")

# Panggil fungsi utama
main_process()