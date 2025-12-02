import pandas as pd
import numpy as np
import os

# ==========================================
# CONFIGURACIÓN DE RUTAS (NUEVA ESTRUCTURA)
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Definimos las rutas a las 3 capas
BRONZE_DIR = os.path.join(DATA_DIR, 'bronze')
SILVER_DIR = os.path.join(DATA_DIR, 'silver')
GOLD_DIR = os.path.join(DATA_DIR, 'gold')

# Aseguramos que las carpetas de destino existan
os.makedirs(SILVER_DIR, exist_ok=True)
os.makedirs(GOLD_DIR, exist_ok=True)

print("🚀 INICIANDO PIPELINE (MODO: CSV DIRECTO)")
print(f"📂 Leyendo desde: {BRONZE_DIR}")
print("-" * 50)

# ==========================================
# FASE 1: LECTURA Y VALIDACIÓN (BRONZE)
# ==========================================
# Nota: Ya no procesamos JSON. Asumimos que los CSV "sucios" están en data/bronze
def get_bronze_data():
    print("\n[PHASE 1] Cargando datos de Capa Bronze...")
    
    meta_path = os.path.join(BRONZE_DIR, '01_Meta_Bronze.csv')
    rev_path = os.path.join(BRONZE_DIR, '01_Reviews_Bronze.csv')

    if not os.path.exists(meta_path) or not os.path.exists(rev_path):
        print("❌ ERROR: No encuentro los archivos en 'data/bronze/'.")
        print(f"   Busqué: {meta_path}")
        print(f"   Busqué: {rev_path}")
        return None, None

    # Leemos los CSVs (low_memory=False ayuda si son grandes)
    df_meta = pd.read_csv(meta_path, low_memory=False)
    df_rev = pd.read_csv(rev_path, low_memory=False)
    
    print(f"   ✅ Meta cargado: {len(df_meta)} registros.")
    print(f"   ✅ Reviews cargado: {len(df_rev)} registros.")
    return df_meta, df_rev

# ==========================================
# FASE 2: MOTOR DE CALIDAD (SILVER)
# ==========================================
def run_silver(df_meta, df_rev):
    print("\n[PHASE 2] Ejecutando Motor de Calidad (Hacia Capa Silver)...")

    # --- A) LIMPIEZA DE PRODUCTOS (CATÁLOGO) ---
    print("   --- [Gobernanza: Catálogo] ---")
    
    # 1. Deduplicación por ID
    duplicados_meta = df_meta.duplicated(subset=['product_id']).sum()
    df_meta_clean = df_meta.drop_duplicates(subset=['product_id'], keep='first')
    print(f"   📉 Duplicados eliminados: {duplicados_meta}")

    # 2. Regla de Negocio: Precio Válido
    # Convertimos a numérico forzando errores a NaN
    df_meta_clean['price'] = pd.to_numeric(df_meta_clean['price'], errors='coerce')
    total_products = len(df_meta_clean)
    # Filtramos (Precio > 0)
    df_meta_clean = df_meta_clean[df_meta_clean['price'] > 0]
    eliminados_precio = total_products - len(df_meta_clean)
    print(f"   💰 Productos con precio inválido/nulo eliminados: {eliminados_precio}")

    # --- B) LIMPIEZA DE REVIEWS ---
    print("   --- [Gobernanza: Reviews] ---")

    # 3. Deduplicación Técnica (Mismo usuario, texto y tiempo)
    cols_dedup = ['product_id', 'text', 'timestamp']
    # Aseguramos que existan las columnas antes de deduplicar
    existing_cols = [c for c in cols_dedup if c in df_rev.columns]
    
    duplicados_rev = df_rev.duplicated(subset=existing_cols).sum()
    df_rev_clean = df_rev.drop_duplicates(subset=existing_cols, keep='first')
    print(f"   👯 Duplicados técnicos eliminados en Reviews: {duplicados_rev}")

    # 4. Integridad de Origen (Verified Purchase)
    total_reviews = len(df_rev_clean)
    # Filtro estricto: Solo True
    df_rev_clean = df_rev_clean[df_rev_clean['verified_purchase'] == True]
    spam_eliminado = total_reviews - len(df_rev_clean)
    print(f"   🛡️ Reseñas NO verificadas (Spam/Bots) eliminadas: {spam_eliminado}")

    # --- GUARDADO EN CARPETA SILVER ---
    path_meta_s = os.path.join(SILVER_DIR, '02_Meta_Silver.csv')
    path_rev_s = os.path.join(SILVER_DIR, '02_Reviews_Silver.csv')
    
    df_meta_clean.to_csv(path_meta_s, index=False)
    df_rev_clean.to_csv(path_rev_s, index=False)
    print(f"   ✅ Archivos Silver guardados en: data/silver/")
    
    return df_meta_clean, df_rev_clean

# ==========================================
# FASE 3: MODELADO DE KPIs (GOLD)
# ==========================================
def run_gold(df_meta_s, df_rev_s):
    print("\n[PHASE 3] Generando Vistas de Negocio (Hacia Capa Gold)...")
    
    # JOIN: Unir transacciones con maestros
    df_gold = pd.merge(df_rev_s, df_meta_s, on='product_id', how='inner')

    # --- KPI 1: OTD (On Time Delivery) ---
    keywords_late = ['late', 'delay', 'did not arrive', 'lost', 'tarde', 'retraso', 'slow']
    df_gold['is_late_delivery'] = df_gold['text'].fillna('').apply(
        lambda x: 1 if any(word in str(x).lower() for word in keywords_late) else 0
    )

    # --- KPI 2: RETURNS (Devoluciones) ---
    keywords_return = ['return', 'refund', 'back', 'devolucion', 'defective', 'broken']
    df_gold['is_return_risk'] = df_gold['text'].fillna('').apply(
        lambda x: 1 if any(word in str(x).lower() for word in keywords_return) else 0
    )

    # Exportar Final
    # Seleccionamos columnas relevantes para el Dashboard
    kpi_cols = ['brand', 'price', 'rating', 'timestamp', 'is_late_delivery', 'is_return_risk', 'product_id']
    existing_cols = [c for c in kpi_cols if c in df_gold.columns]
    
    df_gold_final = df_gold[existing_cols]
    
    path_gold = os.path.join(GOLD_DIR, '03_Dashboard_Gold.csv')
    df_gold_final.to_csv(path_gold, index=False)
    
    print(f"   🏆 [Gold] Dataset Maestro generado con {len(df_gold_final)} registros.")
    print(f"      Ubicación: {path_gold}")

# ==========================================
# EJECUCIÓN PRINCIPAL
# ==========================================
if __name__ == "__main__":
    # 1. Cargar Bronze
    meta, rev = get_bronze_data()
    
    if meta is not None and rev is not None:
        # 2. Ejecutar Silver (Limpieza)
        meta_s, rev_s = run_silver(meta, rev)
        
        # 3. Ejecutar Gold (KPIs)
        run_gold(meta_s, rev_s)
        
        print("\n🎉 PROCESO ETL FINALIZADO CON ÉXITO.")
    else:
        print("\n⚠️ Proceso detenido por falta de archivos Bronze.")