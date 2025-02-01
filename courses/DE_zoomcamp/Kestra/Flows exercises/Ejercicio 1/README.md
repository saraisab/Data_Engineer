## 🔹 Ejercicio 1: Automatizar la Ingesta de Datos en PostgreSQL

📌 Objetivo: Extraer datos desde un archivo CSV en Google Cloud Storage y cargarlos en PostgreSQL.

Pasos:

Sube un archivo CSV a un bucket de Google Cloud Storage (GCS).
Configura un flujo de Kestra para descargar el archivo desde GCS.
Usa una tarea en Kestra para importar el archivo en PostgreSQL con COPY o INSERT.
Verifica en PostgreSQL que los datos se han cargado correctamente.

Tareas de Kestra a usar:

    io.kestra.plugin.gcp.gcs.Download
    io.kestra.plugin.jdbc.postgresql.Execute 


### Explicación del ejercicio

* 1º Creo el bucket con *Terraform*, mediante el archivo main.tf y variables.tf, con esto además de crear el bucket subo a gcs el csv.

En la consola de comandos ejecuto:

`terraform init`

`terraform plan`

`terraform apply`


* 2º Mediante el cuaderno de Jupyter **create_table_schema**.ipynb creo el esquema que posteriormente utilizaré en Kestra

* 3º Creo el flujo **chatgpt.Ex_1_chatgpt_credentials** en Kestra para crear el flujo que me permitirá conectar con Google Cloud Store para descargar el archivo csv

* 4º Creo el flujo **Ex_1_Chatgpt** para descargar el archivo csv, crear la tabla en postgres y rellenarla, por último borrar el csv del flujo de kestra para liberar memoria.

