# RetailMax - Caso academico de mineria de datos (KDD 2026)

Proyecto academico desarrollado para aplicar la metodologia KDD a un caso retail simulado. El objetivo fue traducir una necesidad de negocio en objetivos analiticos, preparar datos transaccionales y dejar una base lista para analisis OLAP.

## Resumen del caso

RetailMax presenta una caida simulada del 8% en el ticket promedio interanual. El proyecto plantea un flujo KDD para identificar combinaciones de productos, segmentos de cliente y canales que ayuden a elevar el ticket promedio sin cambios de precios ni campanas masivas.

## Lo que incluye

- Pipeline ETL ejecutable con Python y pandas.
- Dataset simulado de 50,000 transacciones retail.
- Limpieza de datos, imputacion de segmentos, control de cancelaciones y marcaje de outliers.
- Enriquecimiento con variables temporales, indicadores de ticket, canal digital y temporada.
- Esquema estrella para analisis OLAP con tabla de hechos y dimensiones.
- Metricas de calidad, documentacion del caso y recomendaciones iniciales.

## Estructura

```text
.
├── data/
│   ├── raw/
│   │   ├── datos_retailmax_2026.csv
│   │   └── datos_retailmax_2026.xlsx
│   └── star_schema/
│       ├── fact_ventas.csv
│       ├── dim_tiempo.csv
│       ├── dim_producto.csv
│       ├── dim_cliente.csv
│       ├── dim_canal.csv
│       └── dim_tienda.csv
├── docs/
│   ├── Documentacion_RetailMax.pdf
│   └── Indicaciones_Mineria.pdf
├── metrics/
│   └── metricas_calidad_kdd_2026.json
├── src/
│   └── pipeline_kdd_2026.py
├── requirements.txt
└── README.md
```

## Ejecucion

1. Crear y activar un entorno virtual:

```bash
python -m venv .venv
.venv\Scripts\activate
```

2. Instalar dependencias:

```bash
pip install -r requirements.txt
```

3. Ejecutar el pipeline desde la raiz del repositorio:

```bash
python src/pipeline_kdd_2026.py
```

El script detecta automaticamente el archivo en `data/raw/` y genera las salidas en:

- `metrics/metricas_calidad_kdd_2026.json`
- `data/star_schema/*.csv`

## Metricas principales

- Registros crudos: 50,000
- Registros limpios finales: 37,623
- Tasa de retencion: 75.25%
- Segmentos imputados: 2,747
- Outliers marcados: 232
- Consistencia dimensional: true
- Ticket promedio limpio: $2,508.30
- Segmento con mayor ticket: PyME
- Mes pico identificado: junio

## Modelo dimensional

El proyecto genera un esquema estrella compuesto por:

- `fact_ventas`: tabla de hechos de ventas.
- `dim_tiempo`: calendario y variables temporales.
- `dim_producto`: catalogo de productos, categorias y subcategorias.
- `dim_cliente`: segmentos de cliente.
- `dim_canal`: canal de venta, dispositivo y fuente.
- `dim_tienda`: tiendas, zonas y regiones.

## Tecnologias y metodos

Python, pandas, NumPy, ETL, EDA, KDD, OLAP, esquema estrella, SQL/MySQL.

## Autores

- Alvarado San Juan Antonio
- Martinez Tovar Erik Santiago
- Ramirez Lara Brenda Dominic
- Silva Suarez Anthony Joshua Efren

Materia: Mineria de Datos (7CM77)  
Unidad: UPIICSA - IPN  
Ano: 2026
