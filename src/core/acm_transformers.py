# src/core/acm_transformers.py
import pandas as pd

def transform_from_sheet(df_raw, column_map):
    """
    Transforma datos, valida duplicados y ASEGURA que la columna 'codigo'
    sea un string con formato de ceros a la izquierda.
    """
    df = df_raw.copy()
    rename_map = {k: v for k, v in column_map.items() if k in df.columns}
    df.rename(columns=rename_map, inplace=True)
    
    cols_to_process = list(rename_map.values())
    df = df[cols_to_process]
    
    key_column = cols_to_process[0]
    
    # --- SEGUNDA BARRERA: RE-FORMATEAR EL CÓDIGO ---
    if 'codigo' in df.columns:
        # 1. Asegura que la columna sea de tipo string.
        df['codigo'] = df['codigo'].astype(str)
        # 2. Rellena con ceros a la izquierda hasta tener 4 caracteres.
        #    Esto fuerza al sistema a tratarlo como un texto formateado.
        #    Ajusta el '4' si tus códigos pueden ser más largos (ej. 5 o 6).
        df['codigo'] = df['codigo'].str.zfill(4)

    df.dropna(subset=[key_column], inplace=True)
    
    # ... (El resto de la lógica de validación de duplicados se mantiene igual) ...
    duplicates_df = df[df.duplicated(subset=[key_column], keep=False)]
    if not duplicates_df.empty:
        # ... (código de error de duplicados) ...
        raise ValueError(...)
    
    return df.reset_index(drop=True)

def _asignar_subtemporada(fecha, subtemporadas_df):
    """Función auxiliar para encontrar el ID de la subtemporada basado en una fecha."""
    if pd.isna(fecha): return None
    for _, row in subtemporadas_df.iterrows():
        if isinstance(fecha, pd.Timestamp) and row['fecha_inicio'] <= fecha.date() <= row['fecha_fin']:
            return row['id_subtemporada']
    return None

def transform_obras_and_materiales(df_raw, engine, subtemporadas_df):
    """Procesa los datos de obras y despivota los materiales para su carga."""
    df = df_raw.copy()
    df['fecha_publicacion'] = pd.to_datetime(df['PUBLICACIÓN'], errors='coerce')
    df['id_subtemporada'] = df['fecha_publicacion'].apply(lambda x: _asignar_subtemporada(x, subtemporadas_df))
    
    # Enriquece con id_canal desde la BD
    canales_db = pd.read_sql("SELECT id_canal, codigo FROM canales", engine)
    df = pd.merge(df, canales_db, left_on='COD. CANAL', right_on='codigo', how='left')
    
    # Prepara el DataFrame para la tabla 'obras'
    obras_map = {
        'id_subtemporada': 'id_subtemporada',
        'id_canal': 'id_canal',
        'CODIGO DE OBRA (año/canal/n° obra)': 'codigo_obra',
        'OBRA': 'nombre',
        'PPTO. COSTO TOTAL': 'presupuesto',
        'COSTO REAL TOTAL DELA OBRA': 'real_ejecutado',
        'fecha_publicacion': 'fecha_publicacion'
    }
    df_obras = df[[col for col in obras_map.keys() if col in df.columns]].rename(columns=obras_map)
    df_obras.dropna(subset=['codigo_obra'], inplace=True)

    # Prepara el DataFrame para la tabla 'materialesenobra'
    id_cols = ['CODIGO DE OBRA (año/canal/n° obra)']
    material_cols = [col for col in df.columns if '(' in col and ')' in col and 'COSTO' not in col]
    
    df_melted = df.melt(id_vars=id_cols, value_vars=material_cols, var_name='material_full', value_name='cantidad_usada')
    df_melted = df_melted[df_melted['cantidad_usada'].notna() & (df_melted['cantidad_usada'] > 0)].copy()
    df_melted.rename(columns={'CODIGO DE OBRA (año/canal/n° obra)': 'codigo_obra'}, inplace=True)
    df_melted['nombre_material'] = df_melted['material_full'].apply(lambda x: x.split('(')[0].strip())

    # Enriquece con id_obra y id_material desde la BD
    obras_db = pd.read_sql("SELECT id_obra, codigo_obra FROM obras", engine)
    insumos_db = pd.read_sql("SELECT id_insumo, nombre FROM insumos", engine)
    
    df_final_mats = pd.merge(df_melted, obras_db, on='codigo_obra', how='inner')
    df_final_mats = pd.merge(df_final_mats, insumos_db, left_on='nombre_material', right_on='nombre', how='inner')
    
    # Selecciona solo las columnas finales para la tabla
    df_final_mats = df_final_mats[['id_obra', 'id_insumo', 'cantidad_usada']].copy()

    return {"obras": df_obras, "materiales_en_obra": df_final_mats}