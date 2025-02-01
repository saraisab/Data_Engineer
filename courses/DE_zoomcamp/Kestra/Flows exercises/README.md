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

## 🔹 Ejercicio 2: Ejecutar una Transformación con dbt en BigQuery

📌 Objetivo: Ejecutar un pipeline de transformación en BigQuery usando dbt desde Kestra.
Pasos:

    Crea un dataset en BigQuery y carga datos de prueba.
    Configura dbt en Kestra con las credenciales de GCP.
    Ejecuta un modelo dbt para transformar los datos en BigQuery.
    Guarda el resultado en una tabla particionada y clusterizada.

Tareas de Kestra a usar:

    io.kestra.plugin.dbt.cli.DbtCLI
    io.kestra.plugin.bigquery.Query

## 🔹 Ejercicio 3: Orquestar un Pipeline de ETL con Kestra y Docker

📌 Objetivo: Extraer datos desde una API, procesarlos en un contenedor Docker y guardarlos en PostgreSQL.
Pasos:

    Configura una API pública como fuente de datos (ejemplo: OpenWeather API).
    Crea un contenedor Docker con Python y Pandas para limpiar los datos.
    Usa Kestra para:
        Llamar a la API.
        Ejecutar el contenedor Docker.
        Guardar los datos en PostgreSQL.
    Automatiza el flujo para ejecutarse cada 6 horas.

Tareas de Kestra a usar:

    io.kestra.plugin.core.http.Request
    io.kestra.plugin.scripts.python.Python
    io.kestra.plugin.jdbc.postgresql.Execute

## 🔹 Ejercicio 4: Configurar un Pipeline de Streaming con Pub/Sub y Kestra

📌 Objetivo: Escuchar un tópico de Google Cloud Pub/Sub, procesar los mensajes y guardarlos en BigQuery.
Pasos:

    Crea un tópico en Pub/Sub y publica mensajes JSON.
    Configura Kestra para consumir los mensajes.
    Procesa los datos y almacénalos en BigQuery.
    Verifica los datos en BigQuery.

Tareas de Kestra a usar:

    io.kestra.plugin.gcp.pubsub.Trigger
    io.kestra.plugin.bigquery.Query

## 🔹 Ejercicio 5: Notificaciones Automáticas en Slack o Gmail

📌 Objetivo: Configurar Kestra para enviar alertas cuando haya errores en un pipeline.
Pasos:

    Configura un pipeline de ejemplo (puede ser cualquiera de los anteriores).
    Añade una tarea de notificación para enviar un mensaje si ocurre un error.
    Envía la alerta a Slack o Gmail con Kestra.
    Prueba el sistema generando un error intencional.

Tareas de Kestra a usar:

    io.kestra.plugin.slack.SendMessage
    io.kestra.plugin.gcp.gmail.Send

