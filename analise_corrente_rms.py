# analise_corrente_rms.py
"""
Analisador de Corrente RMS (versão enxuta, focada em tendências de corrente).

Exporta a função:
    analisar_corrente_json(records, tol_perc=0.10, z_thr=3.0)

Entrada aceita:
- lista de {"dadoEnergia": {...}}
- lista de {...} (inner objects)
- um único {...} (inner object)

Saída (apenas dados derivados relacionados a corrente):
- meta (registros, periodo_inicio/fim, nivel_nominal_detectado, tolerancia, timestamps_parsed/invalid)
- tabela_estatisticas (média/std/min/max/amostras por fase)
- grafico_corrente_time_series (série temporal com correntes por registro)
- tendencias_lin (slope + intercept por fase)
- anomalias_indices (índices por fase via z-score)
- events_out_of_limit (subcorrente/sobrecorrente por fase — somente se for possível detectar nível nominal)
"""
from datetime import datetime
from typing import List, Dict, Any, Optional, Union

import numpy as np
import pandas as pd

DATE_OUT_FMT = "%d/%m/%Y %H:%M"  # formato de saída requisitado pelo usuário
# NOTA: não definimos níveis nominais fixos para corrente; detectamos pela mediana das amostras
# Se desejar, pode passar uma lista de níveis nominais como argumento no futuro.

def _normalizar_input(records: Union[List[Any], Dict[str, Any], None]) -> List[Dict[str, Any]]:
    """Normaliza entrada para lista de dicionários internos (conteúdo de dadoEnergia)."""
    if records is None:
        return []
    if isinstance(records, dict):
        if 'dadoEnergia' in records and isinstance(records['dadoEnergia'], dict):
            return [records['dadoEnergia']]
        return [records]
    if isinstance(records, list):
        normalized: List[Dict[str, Any]] = []
        for item in records:
            if not isinstance(item, dict):
                continue
            if 'dadoEnergia' in item and isinstance(item['dadoEnergia'], dict):
                normalized.append(item['dadoEnergia'])
            else:
                normalized.append(item)
        return normalized
    return []

def parse_datetime_series(df: pd.DataFrame, col_name: str = 'data_inc') -> pd.Series:
    """
    Faz parsing robusto da coluna de datas (aceita ISO8601 com Z, etc.)
    Retorna uma Series datetime64[ns] sem tzinfo (UTC).
    """
    if col_name not in df.columns:
        df[col_name] = None
    dt = pd.to_datetime(df[col_name], utc=True, errors='coerce')
    if pd.api.types.is_datetime64tz_dtype(dt):
        dt = dt.dt.tz_convert('UTC').dt.tz_localize(None)
    return dt

def escolher_nivel_nominal_por_mediana(df: pd.DataFrame, fases=('corrente_1', 'corrente_2', 'corrente_3')) -> Optional[float]:
    """
    Detecta um nível nominal de corrente usando a mediana das amostras disponíveis.
    Retorna None se não houver amostras válidas.
    """
    vals = []
    for f in fases:
        if f in df.columns:
            vals.extend(df[f].dropna().tolist())
    if not vals:
        return None
    return float(pd.Series(vals).median())

def estatisticas_por_fase(df: pd.DataFrame, fases=('corrente_1', 'corrente_2', 'corrente_3')) -> List[Dict[str, Any]]:
    tabela = []
    for f in fases:
        if f not in df.columns:
            tabela.append({'Fase': f, 'Média (A)': None, 'Desvio Padrão (A)': None,
                           'Mín (A)': None, 'Máx (A)': None, 'Amostras': 0})
            continue
        s = df[f].dropna()
        if s.empty:
            tabela.append({'Fase': f, 'Média (A)': None, 'Desvio Padrão (A)': None,
                           'Mín (A)': None, 'Máx (A)': None, 'Amostras': 0})
            continue
        tabela.append({
            'Fase': f,
            'Média (A)': float(s.mean()),
            'Desvio Padrão (A)': float(s.std()),
            'Mín (A)': float(s.min()),
            'Máx (A)': float(s.max()),
            'Amostras': int(s.count())
        })
    return tabela

def tendencia_linear_simples(times: pd.Series, series: pd.Series) -> Dict[str, Optional[float]]:
    """
    Ajuste linear simples com verificações robustas para evitar falhas numéricas.
    Retorna None para slope/intercept quando não for possível obter um ajuste estável.
    """
    mask = (~times.isna()) & (~series.isna())
    if mask.sum() < 2:
        return {'slope_per_s': None, 'intercept': None}
    xs = (times[mask] - times[mask].min()).dt.total_seconds().astype(float)
    ys = series[mask].astype(float)

    # remover valores não-finito
    finite_mask = np.isfinite(xs) & np.isfinite(ys)
    if finite_mask.sum() < 2:
        return {'slope_per_s': None, 'intercept': None}
    xs = xs[finite_mask]
    ys = ys[finite_mask]

    # se xs não varia (todos iguais), não faz sentido ajustar
    if np.ptp(xs) == 0:
        return {'slope_per_s': None, 'intercept': None}

    try:
        coeffs = np.polyfit(xs, ys, 1)
        return {'slope_per_s': float(coeffs[0]), 'intercept': float(coeffs[1])}
    except np.linalg.LinAlgError:
        return {'slope_per_s': None, 'intercept': None}
    except Exception:
        return {'slope_per_s': None, 'intercept': None}

def detectar_anomalias_zscore(series: pd.Series, z_thr: float = 3.0) -> List[int]:
    s = series.dropna()
    if s.empty:
        return []
    mu = s.mean()
    sigma = s.std()
    if sigma == 0 or pd.isna(sigma):
        return []
    zs = (series - mu) / sigma
    return [int(i) for i in zs[zs.abs() > z_thr].index.tolist()]

def outside_limits(val: Optional[float], nominal: float, tol_perc: float) -> Optional[str]:
    if val is None or pd.isna(val):
        return None
    low = nominal * (1 - tol_perc)
    high = nominal * (1 + tol_perc)
    if val < low:
        return 'subcorrente'
    if val > high:
        return 'sobrecorrente'
    return 'ok'

def _to_native(o: Any) -> Any:
    """
    Converte recursivamente numpy/pandas/datetime para tipos nativos Python.
    Serializa datetimes no formato dd/mm/aaaa hh:mm.
    """
    if isinstance(o, dict):
        return {str(k): _to_native(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_to_native(v) for v in o]
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        v = float(o)
        return None if pd.isna(v) else v
    if isinstance(o, (np.ndarray,)):
        return [_to_native(v) for v in o.tolist()]
    try:
        if pd.isna(o):
            return None
    except Exception:
        pass
    if isinstance(o, (pd.Timestamp, datetime)):
        try:
            if isinstance(o, pd.Timestamp):
                o2 = o.to_pydatetime()
            else:
                o2 = o
            return o2.strftime(DATE_OUT_FMT)
        except Exception:
            return str(o)
    if isinstance(o, (int, float, str, bool)) or o is None:
        if isinstance(o, float) and pd.isna(o):
            return None
        return o
    return str(o)

def analyze_records_corrente(records: Union[List[Any], Dict[str, Any], None],
                             tol_perc: float = 0.10,
                             z_thr: float = 3.0) -> Dict[str, Any]:
    """
    Produz resultados derivados relacionados a Corrente RMS; NÃO inclui dados de entrada brutos.
    """
    rows = _normalizar_input(records)
    df = pd.DataFrame(rows)

    # garante coluna de data padrão 'data_inc'
    if 'data_inc' not in df.columns:
        df['data_inc'] = None

    # parse robusto de datas (aceita ISO8601 com Z, microssegundos, etc.)
    df['data_inc_dt'] = parse_datetime_series(df, 'data_inc')
    df = df.sort_values('data_inc_dt').reset_index(drop=True)

    # converter colunas numéricas para facilitar cálculos
    for c in df.columns:
        if c not in ['data_inc', 'data_inc_dt', 'id_consumidor', 'id_equipamento']:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    fases = ['corrente_1', 'corrente_2', 'corrente_3']

    nivel_detectado = escolher_nivel_nominal_por_mediana(df, fases)
    tabela = estatisticas_por_fase(df, fases)

    tendencias = {}
    for f in fases:
        series = df.get(f, pd.Series(dtype=float))
        tendencias[f] = tendencia_linear_simples(df['data_inc_dt'], series)

    # série temporal resumida (apenas correntes)
    grafico = []
    for _, r in df.iterrows():
        dt = r['data_inc_dt']
        if pd.isna(dt):
            dt_out = None
        else:
            try:
                dt_out = dt.strftime(DATE_OUT_FMT)
            except Exception:
                dt_out = None
        grafico.append({
            'data_inc': dt_out,
            'corrente_1': None if pd.isna(r.get('corrente_1')) else float(r.get('corrente_1')),
            'corrente_2': None if pd.isna(r.get('corrente_2')) else float(r.get('corrente_2')),
            'corrente_3': None if pd.isna(r.get('corrente_3')) else float(r.get('corrente_3'))
        })

    # eventos de sub/sobrecorrente (apenas se nivel_detectado for detectado)
    events = []
    if nivel_detectado is not None:
        for _, r in df.iterrows():
            dt = r['data_inc_dt']
            if pd.isna(dt):
                dt_out = None
            else:
                try:
                    dt_out = dt.strftime(DATE_OUT_FMT)
                except Exception:
                    dt_out = None
            for f in fases:
                status = outside_limits(r.get(f), nivel_detectado, tol_perc)
                if status in ('subcorrente', 'sobrecorrente'):
                    events.append({
                        'data_inc': dt_out,
                        'fase': f,
                        'valor': None if pd.isna(r.get(f)) else float(r.get(f)),
                        'tipo': status
                    })

    anomalias = {f: detectar_anomalias_zscore(df.get(f, pd.Series(dtype=float)), z_thr=z_thr) for f in fases}

    result = {
        'meta': {
            'registros': int(df.shape[0]),
            'periodo_inicio': df['data_inc_dt'].min().strftime(DATE_OUT_FMT) if not pd.isna(df['data_inc_dt'].min()) else None,
            'periodo_fim': df['data_inc_dt'].max().strftime(DATE_OUT_FMT) if not pd.isna(df['data_inc_dt'].max()) else None,
            'nivel_nominal_detectado': nivel_detectado,
            'tolerancia_perc': tol_perc,
            'timestamps_parsed': int(df['data_inc_dt'].notna().sum()),
            'timestamps_invalid': int(df.shape[0] - df['data_inc_dt'].notna().sum())
        },
        'tabela_estatisticas': {'data': tabela},
        'grafico_corrente_time_series': {'data': grafico},
        'tendencias_lin': tendencias,
        'anomalias_indices': anomalias,
        'events_out_of_limit': events
    }

    return _to_native(result)

def analisar_corrente_json(records: Union[List[Any], Dict[str, Any], None],
                           tol_perc: float = 0.10,
                           z_thr: float = 3.0) -> Dict[str, Any]:
    """
    Função pública: chame com a lista de registros ou com um único registro.
    Retorna somente tipos nativos Python, prontos para json.dumps/Flask.
    """
    return analyze_records_corrente(records, tol_perc=tol_perc, z_thr=z_thr)
