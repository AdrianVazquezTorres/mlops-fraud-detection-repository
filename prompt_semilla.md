Prompt generado por Google Gemini para servir como punto de partida para cada chat nuevo que se abra.

A cada tema/tecnología le corresponderá un único chat de Google Gemini con el objetivo de que el proyecto sea fácilmente escalable, depurable y la busqueda de información sea eficiente y rápida.

Consideraciones:
1. La persona que se encargará de crear el proyecto es alguien con fuertes fundamentos teóricos y prácticos en machine learning, este proyecto es para pasar de ser un data scientist a alguien que domina MLops.


Roadmap de Ejecución MLOps: Proyecto "End-to-End Fraude"

Este no es un plan de lectura, es un plan de construcción. Usaremos el dataset de PaySim (simulación de fraude financiero).

Fase 1: Entorno Seguro y Empaquetado (Semanas 1 - 2)

El objetivo aquí es sacar tu modelo existente del entorno local.

•	Semana 1: Entorno y API REST

o	Acción: Configurar el proyecto usando Poetry para asegurar versiones exactas.

o	Acción: Tomar un modelo XGBoost "tonto" (ya sabes cómo entrenarlo) y crear un script con FastAPI (main.py) que reciba un JSON y devuelva la predicción.

o	Entregable: Una API corriendo en localhost:8000 que responda a peticiones web.

•	Semana 2: Contenerización

o	Acción: Escribir un Dockerfile optimizado (usando caché de capas y .dockerignore).

o	Acción: Construir la imagen y ejecutarla.

o	Entregable: Tu API corriendo dentro de un contenedor Docker, lista para ser enviada a cualquier servidor del mundo.

Fase 2: Trazabilidad y Orquestación Automática (Semanas 3 - 5)

El objetivo es gobernar el modelo y automatizar las tareas repetitivas.

•	Semana 3: Model Governance

o	Acción: Integrar MLflow en tu script de entrenamiento.

o	Acción: Registrar hiperparámetros, el F-beta score y guardar el archivo .pkl automáticamente en el Model Registry de MLflow.

•	Semana 4 y 5: Orquestación de Datos

o	Acción: Levantar Apache Airflow (usando Docker Compose).

o	Acción: Escribir un DAG en Python que automatice tres pasos: 1) Extraer un lote de datos nuevos, 2) Limpiarlos, 3) Hacer inferencia masiva (Batch Scoring) llamando a tu modelo.

o	Entregable: Un pipeline visual en Airflow que se ejecuta todos los días a medianoche sin que tú toques un solo botón.

Fase 3: El Nivel "Manager II" - Streaming y Cloud (Semanas 6 - 8)

El objetivo es procesar fraude en milisegundos y subirlo a la nube (AWS).

•	Semana 6 y 7: Event-Driven Architecture

o	Acción: Levantar un broker de Apache Kafka en un contenedor.

o	Acción: Crear un script en Python (Productor) que envíe 50 transacciones por segundo simulando tarjetas pasando por terminales.

o	Acción: Usar PySpark Structured Streaming para leer (Consumir) ese flujo en vivo, aplicar el modelo XGBoost y detectar el fraude al instante.

•	Semana 8: Despliegue en la Nube (AWS)

o	Acción: Crear cuenta en AWS. Subir los datos masivos a un bucket S3.

o	Acción: Desplegar tu contenedor Docker (el de la Fase 1) en una instancia EC2 o App Runner.

o	Entregable Final: Un repositorio en GitHub impecable, documentado, y una URL pública de AWS donde cualquier reclutador pueda enviar un JSON y recibir una predicción de fraude.

