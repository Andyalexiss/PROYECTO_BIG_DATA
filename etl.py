"""
==================================================================
PROYECTO BIG DATA - EPN
Pipeline ETL: Crime Rate & Safety Analysis
Fases: EXTRACCIÓN -> TRANSFORMACIÓN -> CARGA (Docker/PostgreSQL)
Infraestructura destino: Docker (PostgreSQL) -> Tableau / Looker (GCP)
==================================================================
Principio de diseño: LIMPIEZA NO DESTRUCTIVA.
Ninguna función de este script elimina columnas, ni elimina filas
por el contenido de sus valores (outliers, nulos parciales, etc.).
Solo se descartan:
  - Filas 100% vacías (filas fantasma del CSV)
  - Duplicados EXACTOS fila por fila
Los valores atípicos y nulos se SEÑALAN en columnas nuevas para que
el equipo decida cómo tratarlos en la fase de Visualización, en
lugar de perder registros silenciosamente antes de llegar a Tableau.
==================================================================
"""

import os
import sys
import time

# FORZAR ENTRADA EN INGLÉS: Evita el crash de UnicodeDecodeError de Windows
os.environ['PGLANGUAGE'] = 'en'
os.environ['LC_MESSAGES'] = 'English'
os.environ['LC_ALL'] = 'C'

import pandas as pd
from sqlalchemy import create_engine, text

# ==========================================
# 0. CONFIGURACIÓN GENERAL
# ==========================================
RUTA_CSV = 'crime_rate_safety_analysis-selected-columns.csv'
RUTA_CSV_LIMPIO = 'crime_rate_safety_analysis-limpio.csv'
TABLA_DESTINO = 'delitos'
MIN_REGISTROS_ESPERADOS = 10000  # requisito de la rúbrica del proyecto

URLS_POSTGRES = [
    ("127.0.0.1", "postgresql://grupo_epn:epn_bigdata_2026@127.0.0.1:5555/proyecto_bigdata"),
    ("localhost", "postgresql://grupo_epn:epn_bigdata_2026@localhost:5555/proyecto_bigdata"),
]

# ==========================================
# DICCIONARIOS DE NORMALIZACIÓN (Inglés -> Español)
# Cobertura: los 15 países presentes en el CSV real + países
# adicionales por si el dataset del equipo se amplía.
# ==========================================

DICC_PAISES = {
    # --- Países presentes en el dataset actual ---
    'Australia': 'Australia', 'Brazil': 'Brasil', 'Canada': 'Canadá',
    'Egypt': 'Egipto', 'France': 'Francia', 'Germany': 'Alemania',
    'India': 'India', 'Indonesia': 'Indonesia', 'Mexico': 'México',
    'Nigeria': 'Nigeria', 'Pakistan': 'Pakistán', 'South Africa': 'Sudáfrica',
    'Turkey': 'Turquía', 'UK': 'Reino Unido', 'USA': 'Estados Unidos',
    # --- Cobertura extendida (variantes de nombre / otros países) ---
    'Afghanistan': 'Afganistán', 'Albania': 'Albania', 'Algeria': 'Argelia',
    'Argentina': 'Argentina', 'Austria': 'Austria', 'Bangladesh': 'Bangladés',
    'Belgium': 'Bélgica', 'Bolivia': 'Bolivia', 'Bulgaria': 'Bulgaria',
    'Cambodia': 'Camboya', 'Cameroon': 'Camerún', 'Chile': 'Chile',
    'China': 'China', 'Colombia': 'Colombia', 'Croatia': 'Croacia',
    'Cuba': 'Cuba', 'Czech Republic': 'República Checa', 'Denmark': 'Dinamarca',
    'Ecuador': 'Ecuador', 'El Salvador': 'El Salvador', 'Estonia': 'Estonia',
    'Ethiopia': 'Etiopía', 'Finland': 'Finlandia', 'Ghana': 'Ghana',
    'Greece': 'Grecia', 'Guatemala': 'Guatemala', 'Honduras': 'Honduras',
    'Hungary': 'Hungría', 'Iceland': 'Islandia', 'Iran': 'Irán', 'Iraq': 'Irak',
    'Ireland': 'Irlanda', 'Israel': 'Israel', 'Italy': 'Italia',
    'Ivory Coast': 'Costa de Marfil', 'Jamaica': 'Jamaica', 'Japan': 'Japón',
    'Jordan': 'Jordania', 'Kazakhstan': 'Kazajistán', 'Kenya': 'Kenia',
    'South Korea': 'Corea del Sur', 'North Korea': 'Corea del Norte',
    'Kuwait': 'Kuwait', 'Lebanon': 'Líbano', 'Libya': 'Libia',
    'Malaysia': 'Malasia', 'Morocco': 'Marruecos', 'Mozambique': 'Mozambique',
    'Myanmar': 'Myanmar', 'Nepal': 'Nepal', 'Netherlands': 'Países Bajos',
    'New Zealand': 'Nueva Zelanda', 'Nicaragua': 'Nicaragua', 'Norway': 'Noruega',
    'Panama': 'Panamá', 'Paraguay': 'Paraguay', 'Peru': 'Perú',
    'Philippines': 'Filipinas', 'Poland': 'Polonia', 'Portugal': 'Portugal',
    'Qatar': 'Catar', 'Romania': 'Rumania', 'Russia': 'Rusia',
    'Saudi Arabia': 'Arabia Saudita', 'Serbia': 'Serbia', 'Singapore': 'Singapur',
    'Slovakia': 'Eslovaquia', 'Slovenia': 'Eslovenia', 'Somalia': 'Somalia',
    'Spain': 'España', 'Sri Lanka': 'Sri Lanka', 'Sudan': 'Sudán',
    'Sweden': 'Suecia', 'Switzerland': 'Suiza', 'Syria': 'Siria',
    'Taiwan': 'Taiwán', 'Tanzania': 'Tanzania', 'Thailand': 'Tailandia',
    'Tunisia': 'Túnez', 'Uganda': 'Uganda', 'United Kingdom': 'Reino Unido',
    'Ukraine': 'Ucrania', 'United Arab Emirates': 'Emiratos Árabes Unidos',
    'UAE': 'Emiratos Árabes Unidos', 'United States': 'Estados Unidos',
    'United States of America': 'Estados Unidos', 'US': 'Estados Unidos',
    'U.S.': 'Estados Unidos', 'U.S.A.': 'Estados Unidos', 'Uruguay': 'Uruguay',
    'Uzbekistan': 'Uzbekistán', 'Venezuela': 'Venezuela', 'Vietnam': 'Vietnam',
    'Yemen': 'Yemen', 'Zambia': 'Zambia', 'Zimbabwe': 'Zimbabue',
}

DICC_DELITOS = {
    'Theft': 'Hurto', 'Assault': 'Asalto', 'Robbery': 'Robo',
    'Burglary': 'Allanamiento', 'Fraud': 'Fraude', 'Drug Offense': 'Delito de Drogas',
    'Cybercrime': 'Cibercrimen', 'Domestic Violence': 'Violencia Doméstica',
    'Vandalism': 'Vandalismo', 'Sexual Assault': 'Agresión Sexual',
    'Murder': 'Homicidio', 'Extortion': 'Extorsión', 'Arson': 'Incendio Provocado',
    'Kidnapping': 'Secuestro', 'Trafficking': 'Tráfico de Personas',
    # Cobertura extendida por si el dataset se amplía
    'Vehicle Theft': 'Robo de Vehículo', 'Homicide': 'Homicidio',
    'Carjacking': 'Robo de Vehículo con Violencia', 'Money Laundering': 'Lavado de Dinero',
    'Bribery': 'Soborno', 'Terrorism': 'Terrorismo', 'Smuggling': 'Contrabando',
}

DICC_AREA = {'Urban': 'Urbano', 'Rural': 'Rural', 'Suburban': 'Suburbano', 'Remote': 'Remoto'}

DICC_ESTACIONES = {'Winter': 'Invierno', 'Spring': 'Primavera', 'Summer': 'Verano',
                    'Autumn': 'Otoño', 'Fall': 'Otoño'}

DICC_DIAS = {'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles',
             'Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'}

DICC_TIEMPO = {
    'Morning (6am-12pm)': 'Mañana (6am-12pm)',
    'Afternoon (12pm-6pm)': 'Tarde (12pm-6pm)',
    'Evening (6pm-10pm)': 'Anochecer (6pm-10pm)',
    'Night (10pm-2am)': 'Noche (10pm-2am)',
    'Late Night (2am-6am)': 'Madrugada (2am-6am)',
}

DICC_GENERO = {'Female': 'Femenino', 'Male': 'Masculino', 'Unknown': 'Desconocido'}

DICC_EDAD = {'Under 18': 'Menor de 18'}  # los demás rangos (18-25, 26-35, ...) no requieren traducción

DICC_ARMA = {
    'Firearm': 'Arma de Fuego', 'Knife': 'Arma Blanca', 'Blunt Object': 'Objeto Contundente',
    'Chemical': 'Sustancia Química', 'Vehicle': 'Vehículo', 'Unknown': 'Desconocido',
}

MESES_ES = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
            7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}


# ==========================================
# 1. FASE DE EXTRACCIÓN
# ==========================================
def extraer_datos(ruta: str) -> pd.DataFrame:
    print("[ETL] Iniciando la extracción del dataset...")
    try:
        df = pd.read_csv(ruta)
    except FileNotFoundError:
        print(f"ERROR CRÍTICO: No se encontró '{ruta}' en el directorio de trabajo.")
        sys.exit(1)
    except UnicodeDecodeError:
        print("[ETL] Encoding UTF-8 falló, reintentando con latin-1...")
        df = pd.read_csv(ruta, encoding='latin-1')

    print(f"[ETL] Extracción completa: {len(df):,} filas, {len(df.columns)} columnas.")
    if len(df) < MIN_REGISTROS_ESPERADOS:
        print(f"[ETL] ADVERTENCIA: se esperaban ≥{MIN_REGISTROS_ESPERADOS:,} registros "
              f"(rúbrica del proyecto) y se encontraron {len(df):,}.")
    return df


# ==========================================
# 2. FASE DE TRANSFORMACIÓN
# ==========================================
def sanear_estructura(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina SOLO filas 100% vacías y duplicados exactos. No toca columnas."""
    print("[ETL] Analizando integridad estructural del dataset...")
    filas_iniciales = len(df)

    df = df.dropna(how='all')
    vacias = filas_iniciales - len(df)

    if len(df) == 0:
        print("ERROR CRÍTICO: El archivo CSV no contiene registros. Operación abortada.")
        sys.exit(1)

    antes_dup = len(df)
    df = df.drop_duplicates()
    duplicadas = antes_dup - len(df)

    print(f"[ETL] Filas fantasma eliminadas: {vacias} | Duplicados exactos eliminados: {duplicadas}")
    print(f"[ETL] CONFIRMACIÓN: {len(df):,} filas reales listas para transformar "
          f"({len(df.columns)} columnas, ninguna eliminada).")
    return df


def sanitizar_texto(df: pd.DataFrame) -> pd.DataFrame:
    """Quita espacios/tabs invisibles en columnas de texto (previene fallos en WHERE/GROUP BY)."""
    columnas_texto = df.select_dtypes(include=['object', 'string']).columns
    for col in columnas_texto:
        df[col] = df[col].astype(str).str.strip()
    print(f"[ETL] Texto sanitizado en {len(columnas_texto)} columnas de tipo texto.")
    return df


def normalizar_categorias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Traduce categorías EN->ES de forma no destructiva: si un valor no está
    en el diccionario, se conserva tal cual (no se pierde información) y
    se reporta para que el equipo lo revise.
    """
    mapeos = {
        'country': DICC_PAISES, 'crime_type': DICC_DELITOS, 'area_type': DICC_AREA,
        'season': DICC_ESTACIONES, 'day_of_week': DICC_DIAS, 'time_of_day': DICC_TIEMPO,
        'victim_gender': DICC_GENERO, 'victim_age_group': DICC_EDAD, 'weapon_used': DICC_ARMA,
    }

    for columna, diccionario in mapeos.items():
        if columna not in df.columns:
            continue
        valores_presentes = set(df[columna].dropna().unique())
        no_mapeados = valores_presentes - set(diccionario.keys())
        if no_mapeados:
            print(f"[ETL] AVISO: valores de '{columna}' sin traducción en el diccionario "
                  f"(se conservan en inglés): {sorted(no_mapeados)}")
        df[columna] = df[columna].replace(diccionario)

    # Columna derivada: nombre del mes en español (no reemplaza la columna 'month' original)
    if 'month' in df.columns:
        df['mes_nombre'] = df['month'].map(MESES_ES)

    print("[ETL] Normalización lingüística (EN->ES) aplicada sobre categorías.")
    return df


def tratar_nulos_sin_eliminar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rellena nulos con una categoría explícita en lugar de eliminar filas o
    columnas. Un nulo en 'weapon_used' es información real (no se usó arma).
    """
    nulos_antes = df.isnull().sum()
    columnas_con_nulos = nulos_antes[nulos_antes > 0]

    if 'weapon_used' in df.columns:
        df['weapon_used'] = df['weapon_used'].fillna('Sin Arma Registrada')

    nulos_despues = df.isnull().sum()

    if len(columnas_con_nulos) > 0:
        print("[ETL] Tratamiento de nulos (sin eliminar filas/columnas):")
        for col in columnas_con_nulos.index:
            print(f"       - {col}: {nulos_antes[col]} nulos -> {nulos_despues[col]} nulos restantes")
    else:
        print("[ETL] No se detectaron valores nulos en el dataset.")
    return df


def auditar_calidad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida rangos lógicos y detecta outliers vía IQR. Los casos atípicos se
    SEÑALAN en columnas booleanas nuevas; NINGUNA fila se elimina por esto,
    para que la decisión de filtrarlos o no se tome en Tableau/Looker.
    """
    print("[ETL] Auditoría de calidad de datos (validación de rangos y outliers)...")

    # Rango de años observado vs. razonable
    if 'year' in df.columns:
        rango = (df['year'].min(), df['year'].max())
        print(f"       - Rango de 'year': {rango}")

    # Consistencia lógica: fatalities no debería superar victim_count
    if {'fatalities', 'victim_count'}.issubset(df.columns):
        inconsistentes = (df['fatalities'] > df['victim_count']).sum()
        df['inconsistencia_victimas'] = df['fatalities'] > df['victim_count']
        print(f"       - Registros con fatalities > victim_count (señalados, no eliminados): {inconsistentes}")

    # Outliers de pérdida financiera vía rango intercuartílico (IQR)
    if 'financial_loss_usd' in df.columns:
        q1, q3 = df['financial_loss_usd'].quantile([0.25, 0.75])
        iqr = q3 - q1
        limite_superior = q3 + 1.5 * iqr
        df['perdida_atipica'] = df['financial_loss_usd'] > limite_superior
        print(f"       - Outliers de 'financial_loss_usd' (> {limite_superior:,.2f} USD) "
              f"señalados: {df['perdida_atipica'].sum()}")

    # Columna derivada útil para Tableau: nivel de severidad categórico
    if 'crime_severity_score' in df.columns:
        df['nivel_severidad'] = pd.cut(
            df['crime_severity_score'],
            bins=[0, 3.33, 6.66, 10],
            labels=['Baja', 'Media', 'Alta'],
            include_lowest=True,
        )

    return df


# ==========================================
# 3. FASE DE CARGA (Docker / PostgreSQL)
# ==========================================
def conectar_postgres(urls_a_probar):
    print("[ETL] Conectando al motor relacional PostgreSQL en Docker...")
    for nombre, url in urls_a_probar:
        print(f"       Intentando enlace de red por: {nombre}...")
        try:
            engine = create_engine(url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))  # valida que el motor responde de verdad
            print(f"[ETL] Conexión establecida con éxito a través de {nombre}.")
            return engine
        except Exception as e:
            print(f"       Enlace por {nombre} no disponible: {e}")

    print("ERROR CRÍTICO: No se pudo establecer conexión con la base de datos "
          "a través de ningún puerto de red local. Verifica que el contenedor "
          "Docker de PostgreSQL esté corriendo (docker ps).")
    sys.exit(1)


def cargar_a_postgres(df: pd.DataFrame, engine, tabla: str):
    print(f"[ETL] Inyectando datos en la tabla '{tabla}'...")
    try:
        df.to_sql(tabla, engine, if_exists='replace', index=False, chunksize=1000, method='multi')
    except Exception as e:
        print(f"ERROR durante la escritura física en el DBMS: {e}")
        sys.exit(1)

    # Verificación real de que los datos quedaron almacenados (demo para la rúbrica)
    with engine.connect() as conn:
        total_bd = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()
        print(f"[ETL] VERIFICACIÓN: la tabla '{tabla}' contiene {total_bd:,} registros en PostgreSQL.")

        if 'country' in df.columns:
            resultado = conn.execute(
                text(f"SELECT country, COUNT(*) AS total FROM {tabla} GROUP BY country ORDER BY total DESC LIMIT 5")
            )
            print("[ETL] Muestra de verificación (top 5 países cargados):")
            for fila in resultado:
                print(f"       - {fila.country}: {fila.total} registros")


# ==========================================
# MAIN
# ==========================================
def main():
    inicio = time.time()

    df = extraer_datos(RUTA_CSV)
    columnas_originales = len(df.columns)

    df = sanear_estructura(df)
    df = sanitizar_texto(df)
    df = normalizar_categorias(df)
    df = tratar_nulos_sin_eliminar(df)
    df = auditar_calidad(df)

    assert len(df.columns) >= columnas_originales, "Se perdieron columnas durante la limpieza."

    # Respaldo local del dataset limpio (útil para el entregable .zip del dataset)
    df.to_csv(RUTA_CSV_LIMPIO, index=False, encoding='utf-8-sig')
    print(f"[ETL] Respaldo CSV limpio exportado: {RUTA_CSV_LIMPIO} ({len(df):,} filas)")

    engine = conectar_postgres(URLS_POSTGRES)
    cargar_a_postgres(df, engine, TABLA_DESTINO)

    duracion = time.time() - inicio
    print(f"[ETL] PROCESO COMPLETADO en {duracion:.2f}s. "
          f"{len(df):,} registros y {len(df.columns)} columnas listos para Tableau/Looker.")


if __name__ == '__main__':
    main()