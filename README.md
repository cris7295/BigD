# Amazon Data Governance & Quality Framework (ETL Pipeline)

## 📌 Descripción del Proyecto
Este repositorio contiene la implementación técnica del Trabajo de Investigación Final: **"Gobernanza y Calidad de datos para el Ecosistema de e-commerce a Gran Escala"**.

El proyecto consiste en un pipeline **ETL (Extract, Transform, Load)** desarrollado en **Python** que implementa una **Arquitectura Medallion** (Bronze $\to$ Silver $\to$ Gold). Su objetivo es tomar datos transaccionales crudos (CSVs), aplicar un estricto motor de gobernanza (deduplicación, validación de precios, integridad de origen) y generar datasets analíticos para KPIs de negocio.

## 📂 Estructura del Proyecto
El código requiere la siguiente estructura de directorios para funcionar:

```text
/
├── data/
│   ├── bronze/             # [Fuente] Colocar aquí los CSVs crudos (01_..._Bronze.csv)
│   ├── silver/             # [Destino] Aquí se guardan los datos limpios
│   └── gold/               # [Destino] Aquí se genera el Dashboard de KPIs
│
├── etl_pipeline.py         # Script principal de ejecución (Orquestador)
└── README.md               # Documentación técnica

⚙️ Requisitos Técnicos
Python 3.8+

Librerías: pandas, numpy

Espacio en disco: Al menos 500MB libres.

🚀 Instrucciones de Ejecución
1. Preparación de Datos (Capa Bronze)
El script asume que los datos crudos ya han sido extraídos y convertidos a CSV.

Paso Crítico: Debes crear manualmente la carpeta data/bronze/ y pegar dentro los siguientes archivos fuente:

01_Meta_Bronze.csv (Catálogo de Productos sucio)

01_Reviews_Bronze.csv (Transacciones/Reseñas sucias)

2. Instalación de Dependencias
Ejecutar en la terminal de Visual Studio Code:

Bash

pip install pandas numpy
3. Ejecutar el Pipeline
El sistema detectará los archivos en Bronze y procesará las capas Silver y Gold automáticamente.

Bash

python etl_pipeline.py
🧠 Arquitectura de Datos (Medallion)
El pipeline procesa la información en tres capas secuenciales:

🥉 Capa Bronze (Raw Input)
Ubicación: data/bronze/

Estado: Datos crudos "As-Is". Se preservan duplicados, precios nulos y spam para tener una línea base de auditoría.

🥈 Capa Silver (Gobernanza & Calidad)
Ubicación: data/silver/

Motor de Calidad:

Unicidad: Deduplicación por ID de producto y huella digital de la reseña (usuario+texto+tiempo).

Consistencia de Negocio: Eliminación de productos con price <= 0.

Integridad de Origen: Filtrado estricto donde verified_purchase == True. Se descarta tráfico de bots/spam.

🥇 Capa Gold (Valor & KPIs)
Ubicación: data/gold/

Archivo Final: 03_Dashboard_Gold.csv

Modelado: Join entre Catálogo y Reviews + Enriquecimiento con NLP.

KPIs Generados:

OTD (On-Time Delivery): Detección de palabras clave como "late", "delay".

Riesgo de Devolución: Detección de intención de retorno ("return", "broken").
