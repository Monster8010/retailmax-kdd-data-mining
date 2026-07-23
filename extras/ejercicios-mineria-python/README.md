# Ejercicios de mineria de datos en Python

Material complementario extraido de `Ejercicios_Mineria_Python.rar`.

## Contenido

- `Mineria1Ejercicios/notebooks/01_ejercicio_kdd.ipynb`
- `Mineria1Ejercicios/notebooks/02_ejercicio_etl_olap.ipynb`
- `Mineria1Ejercicios/notebooks/03_ejercicio_eda.ipynb`
- `Mineria1Ejercicios/notebooks/eda_temporal_tickets.png`
- `Mineria1Ejercicios/notebooks/eda_visualizacion.png`
- `Mineria1Ejercicios/requirements.txt`

## Nota sobre el RAR original

El archivo RAR completo no se subio directamente porque pesa mas de 100 MiB, limite que GitHub bloquea para archivos individuales en repositorios Git normales. Ademas, el RAR incluia una carpeta `.venv` con dependencias instaladas, caches y binarios locales.

Para mantener el repositorio portable, se subio el contenido academico util y se omitio el entorno virtual. Para ejecutar los notebooks, crea un entorno nuevo e instala las dependencias:

```bash
cd extras/ejercicios-mineria-python/Mineria1Ejercicios
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
