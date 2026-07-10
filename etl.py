import os
# FORZAR ENTRADA EN INGLÉS: Evita el crash de UnicodeDecodeError de Windows de forma nativa
os.environ['PGLANGUAGE'] = 'en'
os.environ['LC_MESSAGES'] = 'English'
os.environ['LC_ALL'] = 'C'

import pandas as pd
from sqlalchemy import create_engine

# ==========================================
# 1. FASE DE EXTRACCIÓN 
# ==========================================
print("[ETL] Iniciando la extracción del dataset...")
try:
    # Se realiza la lectura directa y completa de todas las filas y columnas del CSV
    df = pd.read_csv('crime_rate_safety_analysis-selected-columns.csv')
except FileNotFoundError:
    print("ERROR CRÍTICO: No se encontró el archivo CSV en el directorio de trabajo.")
    exit()

# ==========================================
# 2. FASE DE TRANSFORMACIÓN & SANITIZACIÓN
# ==========================================
print("[ETL] Analizando integridad estructural del Dataset...")

# Eliminamos únicamente las filas que estén completamente vacías (filas fantasma del CSV)
df_clean = df.dropna(how='all')

# Control de ejecución estricto: si el archivo físico no tiene registros, el pipeline se detiene
if len(df_clean) == 0:
    print("ERROR CRÍTICO: El archivo CSV no contiene registros. Operación abortada.")
    exit()

print(f"CONFIRMACIÓN: Dataset válido detectado. Procesando {len(df_clean)} filas reales...")

# Eliminamos registros duplicados exactos para asegurar la unicidad estadística
df_clean = df_clean.drop_duplicates()

# Sanitización de texto: Remoción de espacios en blanco invisibles (\t, \r, espacios) 
# al inicio y final de las cadenas en todas las columnas de tipo objeto/texto.
# Esto previene que falles las cláusulas WHERE o GROUP BY en PostgreSQL y Tableau.
objeto_cols = df_clean.select_dtypes(include=['object', 'string']).columns
for col in objeto_cols:
    df_clean[col] = df_clean[col].astype(str).str.strip()

# Diccionarios de Normalización y Traducción Lingüística (Inglés -> Español)
dicc_paises = {
    'United States': 'Estados Unidos', 'Spain': 'España', 'Mexico': 'México',
    'Germany': 'Alemania', 'France': 'Francia', 'United Kingdom': 'Reino Unido',
    'Canada': 'Canadá', 'Brazil': 'Brasil', 'Italy': 'Italia', 'Japan': 'Japón',
    'India': 'India', 'Australia': 'Australia', 'China': 'China', 'Russia': 'Rusia'
}

dicc_delitos = {
    'Theft': 'Hurto', 'Assault': 'Asalto', 'Vehicle Theft': 'Robo de Vehículo',
    'Fraud': 'Fraude', 'Robbery': 'Robo', 'Burglary': 'Allanamiento', 'Murder': 'Homicidio'
}

dicc_tiempo = {
    'Morning': 'Mañana', 'Afternoon': 'Tarde', 'Night': 'Noche', 'Evening': 'Noche'
}

dicc_area = {
    'Urban': 'Urbano', 'Rural': 'Rural'
}

dicc_estaciones = {
    'Winter': 'Invierno', 'Spring': 'Primavera', 'Summer': 'Verano', 'Autumn': 'Otoño', 'Fall': 'Otoño'
}

dicc_meses = {
    'January': 'Enero', 'February': 'Febrero', 'March': 'Marzo', 'April': 'Abril',
    'May': 'Mayo', 'June': 'Junio', 'July': 'Julio', 'August': 'Agosto',
    'September': 'Septiembre', 'October': 'Octubre', 'November': 'Noviembre', 'December': 'Diciembre'
}

dicc_dias = {
    'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles', 'Thursday': 'Jueves',
    'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
}

# Aplicación segura de diccionarios mediante reemplazo no destructivo.
# Si un país o columna no está explícitamente en el diccionario, se conserva su valor original del CSV.
if 'country' in df_clean.columns:
    df_clean['country'] = df_clean['country'].replace(dicc_paises)
    
if 'crime_type' in df_clean.columns:
    df_clean['crime_type'] = df_clean['crime_type'].replace(dicc_delitos)
    
if 'time_of_day' in df_clean.columns:
    df_clean['time_of_day'] = df_clean['time_of_day'].replace(dicc_tiempo)
    
if 'area_type' in df_clean.columns:
    df_clean['area_type'] = df_clean['area_type'].replace(dicc_area)
    
if 'season' in df_clean.columns:
    df_clean['season'] = df_clean['season'].replace(dicc_estaciones)
    
if 'month' in df_clean.columns:
    df_clean['month'] = df_clean['month'].replace(dicc_meses)
    
if 'day_of_week' in df_clean.columns:
    df_clean['day_of_week'] = df_clean['day_of_week'].replace(dicc_dias)

df_final = df_clean
print(f"[ETL] Transformación concluida con éxito. {len(df_final)} registros listos para persistencia.")

# ==========================================
# 3. FASE DE CARGA (PERSISTENCIA EN DOCKER)
# ==========================================
print("[ETL] Conectando al motor relacional PostgreSQL en Docker...")

# Pasarela de redundancia para la resolución de nombres de red locales en Windows/WSL2
urls_a_probar = [
    ("127.0.0.1", "postgresql://grupo_epn:epn_bigdata_2026@127.0.0.1:5555/proyecto_bigdata"),
    ("localhost", "postgresql://grupo_epn:epn_bigdata_2026@localhost:5555/proyecto_bigdata")
]

engine = None
conexion_exitosa = False

for nombre, url in urls_a_probar:
    print(f"Intentando enlace de red por: {nombre}...")
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            pass
        print(f"Conexión establecida con éxito a través de {nombre}")
        conexion_exitosa = True
        break
    except Exception as e:
        print(f"Enlace por {nombre} no disponible. Pasando al siguiente recurso.\n")

if not conexion_exitosa:
    print("ERROR CRÍTICO: No se pudo establecer la conexión con la base de datos a través de ningún puerto de red local.")
    exit()

# Inyección final de la data purificada del CSV
try:
    print("Inyectando datos reales en la tabla 'delitos'...")
    # if_exists='replace' garantiza la idempotencia del script recreando la estructura limpia
    df_final.to_sql('delitos', engine, if_exists='replace', index=False)
    print("[ETL] PROCESO COMPLETADO: La base de datos relacional está actualizada con datos 100% reales.")
except Exception as e:
    print(f"ERROR durante la escritura física en el DBMS: {e}")