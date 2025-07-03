import pandas as pd

# --- Transformadores para Datos Maestros ---

def transform_canales_from_obras(df_obras_raw):
    """Extrae, limpia y devuelve un DataFrame único de Canales desde la planilla de Obras."""
    
    # Seleccionamos las columnas que nos interesan
    df_canales = df_obras_raw[['CANAL', 'SECCION']].copy()
    
    # --- LÍNEA CLAVE AÑADIDA ---
    # Eliminamos cualquier fila donde la columna 'CANAL' esté vacía.
    # Esto previene el error de NotNullViolation.
    df_canales.dropna(subset=['CANAL'], inplace=True)
    
    # Renombramos las columnas para que coincidan con la base de datos.
    df_canales.rename(columns={'CANAL': 'nombre', 'SECCION': 'sector'}, inplace=True)
    
    # Finalmente, eliminamos duplicados para tener una lista única de canales.
    return df_canales.drop_duplicates(subset=['nombre']).reset_index(drop=True)

def transform_materiales_from_obras_columns(df_obras_raw):
    """Despivota los nombres de las columnas de materiales para crear una tabla maestra."""
    material_cols = [col for col in df_obras_raw.columns if '(' in col and ')' in col]
    
    materiales_list = []
    for col in material_cols:
        try:
            nombre = col.split('(')[0].strip()
            unidad = col.split('(')[1].replace(')', '').strip()
            materiales_list.append({'nombre': nombre, 'unidad': unidad})
        except IndexError:
            print(f"Advertencia: La columna '{col}' no sigue el formato 'Nombre (Unidad)' y será ignorada.")
            continue
            
    return pd.DataFrame(materiales_list).drop_duplicates(subset=['nombre']).reset_index(drop=True)

# --- Transformadores para Datos Transaccionales ---

def _asignar_subtemporada(fecha, subtemporadas_df):
    """Función auxiliar para encontrar el ID de la subtemporada basado en una fecha."""
    if pd.isna(fecha):
        return None
    for _, fila in subtemporadas_df.iterrows():
        if fila['fecha_inicio'] <= fecha.date() <= fila['fecha_fin']:
            return fila['id_subtemporada']
    return None

def transform_obras_and_materiales(df_raw, engine, subtemporadas_df):
    """Transforma Obras y despivota sus Materiales para crear MaterialesEnObra."""
    df_raw['fecha_publicacion'] = pd.to_datetime(df_raw['fecha_publicacion'], errors='coerce')
    df_raw['id_subtemporada'] = df_raw['fecha_publicacion'].apply(lambda x: _asignar_subtemporada(x, subtemporadas_df))
    
    # Mapeo de columnas y limpieza de Obras
    # (Añade aquí el resto de las columnas que necesites de tu Excel)
    df_obras = df_raw[['id_subtemporada', 'CODIGO', 'OBRA', 'Ppto', 'Real Ejecutado', 'fecha_publicacion']].copy()
    df_obras.rename(columns={
        'CODIGO': 'codigo_obra',
        'OBRA': 'nombre',
        'Ppto': 'presupuesto',
        'Real Ejecutado': 'real_ejecutado'
    }, inplace=True)
    
    # Obtener id_canal
    canales_db = pd.read_sql("SELECT id_canal, nombre FROM canales", engine)
    df_obras = pd.merge(df_obras, canales_db, left_on='Nombre del Canal', right_on='nombre', how='left')
    df_obras.drop(columns=['nombre_y'], inplace=True) # Limpiar columna duplicada
    df_obras.rename(columns={'nombre_x': 'nombre'}, inplace=True)


    # --- Despivotar Materiales para MaterialesEnObra ---
    id_cols = ['CODIGO']
    material_cols = [col for col in df_raw.columns if '(' in col and ')' in col]
    
    df_melted = df_raw.melt(
        id_vars=id_cols,
        value_vars=material_cols,
        var_name='material_full',
        value_name='cantidad_usada'
    )
    
    df_melted = df_melted[df_melted['cantidad_usada'].notna() & (df_melted['cantidad_usada'] > 0)].copy()
    df_melted['nombre_material'] = df_melted['material_full'].apply(lambda x: x.split('(')[0].strip())

    # Obtener IDs de la base de datos para crear las relaciones
    obras_db = pd.read_sql("SELECT id_obra, codigo_obra FROM Obras", engine)
    materiales_db = pd.read_sql("SELECT id_material, nombre FROM materiales", engine)
    
    df_final_materiales = pd.merge(df_melted, obras_db, left_on='CODIGO', right_on='codigo_obra')
    df_final_materiales = pd.merge(df_final_materiales, materiales_db, left_on='nombre_material', right_on='nombre')
    
    df_final_materiales = df_final_materiales[['id_obra', 'id_material', 'cantidad_usada']].copy()

    return {
        "obras": df_obras,
        "materiales_en_obra": df_final_materiales
    }