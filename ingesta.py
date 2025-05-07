import csv
import boto3
import mysql.connector

# Configuración MySQL desde variables de entorno
db_host     = "172.31.28.92"
db_port     = "8005"
db_user     = "root"
db_password = "utec"
db_name     = "bd_api_employees"

# Nombre del CSV de salida
fichero_upload = "dataMYSQL.csv"
# Bucket S3 (por defecto o desde variable de entorno)
nombre_bucket  = "arb-aoutput-01"

# Función para extraer datos de MySQL
def fetch_data():
    conn = mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employees")
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    cursor.close()
    conn.close()
    return columns, rows

# Guardar los datos en CSV
def save_to_csv(columns, rows, filename):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    return filename

# Subir el CSV a S3
def upload_to_s3(file_path):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, nombre_bucket, file_path)
    print("Ingesta completada")

if __name__ == '__main__':
    cols, data = fetch_data()
    print(cols, data)
    csv_file   = save_to_csv(cols, data, fichero_upload)
    print(csv_file)
    upload_to_s3(csv_file)
