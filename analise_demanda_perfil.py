# analise_demanda_perfil.py
"""
Analisador de Perfil Diário e Horário de Demanda.

Exporta a função:
    analisar_demanda_json(records, field_name='potencia_ativa_tot', agg='hour', tol_perc=0.10, z_thr=3.0)

Principais saídas:
- meta: contagem de registros, período, timestamps parseados/invalid
- estatísticas básicas sobre demanda (média/std/min/max/amostras)
- perfil_horario: média da demanda por hora do dia (0..23)
- perfil_diario: média da demanda por dia da semana (0=segunda..? ou 0=segunda conforme pandas)
- time_series_agg: série temporal agregada (por hora ou dia) com timestamps no formato dd/mm/aaaa hh:mm
- picos: horas/dias com maiores demandas (top N)
- tendencias_lin: tendência da série agregada
- anomalias_indices: índices na série agregada detectados por z-score
- events_out_of_limit: eventos quando demanda ultrapassa tolerância relativa à mediana
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Tuple

import numpy as np
import pandas as pd

DATE_OUT_FMT = "%d/%m/%Y %H:%M"


def _normalizar_input(records: Union[List[Any], Dict[str, Any], None]) -> List[Dict[str, Any]]:
    if records is None:
        return []
    if isinstance(records, dict):
        if 'dadoEnergia' in records and isinstance(records['dadoEnergia'], dict):
            return [records['dadoEnergia']]
        return [records]
    if isinstance(records, list):
        result: List[Dict[str, Any]] = []
        for item in records:
            if not isinstance(item, dict):
                continue
            if 'dadoEnergia' in item and isinstance(item['dadoEnergia'], dict):
                result.append(item['dadoEnergia'])
            else:
                result.append(item)
        return result
    return []


def parse_datetime_series(df: pd.DataFrame, col_name: str = 'data_inc') -> pd.Series:
    if col_name not in df.columns:
        df[col_name] = None
    dt = pd.to_datetime(df[col_name], utc=True, errors='coerce')
    if pd.api.types.is_datetime64tz_dtype(dt):
        dt = dt.dt.tz_convert('UTC').dt.tz_localize(None)
    return dt


def tendencia_linear_simples(times: pd.Series, series: pd.Series) -> Dict[str, Optional[float]]:
    mask = (~times.isna()) & (~series.isna())
    if mask.sum() < 2:
        return {'slope_per_s': None, 'intercept': None}
    xs = (times[mask] - times[mask].min()).dt.total_seconds().astype(float)
    ys = series[mask].astype(float)

    finite_mask = np.isfinite(xs) & np.isfinite(ys)
    if finite_mask.sum() < 2:
        return {'slope_per_s': None, 'intercept': None}
    xs = xs[finite_mask]
    ys = ys[finite_mask]

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


def _to_native(o: Any) -> Any:
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


def _choose_demand_field(df: pd.DataFrame, field_name: Optional[str]) -> Optional[str]:
    """
    Determina qual coluna usar como 'demanda' (potência ativa total, potência_ap_tot, etc.)
    Ordem de preferência:
      - field_name se fornecido e existe
      - 'potencia_ativa_tot'
      - 'potencia_ap_tot'
      - soma das fases 'potencia_ativa_1/2/3' se presentes
    Retorna nome da coluna (ou None se não encontrar).
    """
    if field_name and field_name in df.columns:
        return field_name
    candidates = ['potencia_ativa_tot', 'potencia_ap_tot']
    for c in candidates:
        if c in df.columns:
            return c
    # tenta somar fases
    fases = ['potencia_ativa_1', 'potencia_ativa_2', 'potencia_ativa_3']
    if all(f in df.columns for f in fases):
        return None  # sinaliza que usaremos soma direta (não uma coluna única)
    return None


def _compute_demand_series(df: pd.DataFrame, chosen_field: Optional[str]) -> pd.Series:
    """
    Retorna uma Series (index alinhado a df.index) com valores de demanda (float) ou NaN.
    Se chosen_field is None e existem colunas de fase, retorna soma por linha.
    """
    if chosen_field is not None and chosen_field in df.columns:
        return df[chosen_field].astype(float)
    # tenta soma das fases quando possível
    fase_cols = [c for c in ['potencia_ativa_1', 'potencia_ativa_2', 'potencia_ativa_3'] if c in df.columns]
    if fase_cols:
        return df[fase_cols].sum(axis=1, min_count=1)
    # fallback: busca coluna aproximada
    for alt in ['potencia_ap_tot', 'potencia_ap_1', 'potencia_ap_2', 'potencia_ap_3']:
        if alt in df.columns:
            return df[alt].astype(float)
    # nada encontrado -> série vazia
    return pd.Series([np.nan] * len(df), index=df.index, dtype=float)


def aggregate_time_series(df: pd.DataFrame, dt_col: str, demand_series: pd.Series, agg: str = 'hour') -> Tuple[pd.DatetimeIndex, pd.Series]:
    """
    Agrega a série de demanda por hora ou por dia.
    Retorna (index, series) onde index são timestamps (pandas.DatetimeIndex) e series valores agregados (média).
    agg: 'hour' or 'day'
    """
    tmp = pd.DataFrame({'dt': df[dt_col], 'val': demand_series})
    tmp = tmp.dropna(subset=['dt'])
    if tmp.empty:
        return pd.DatetimeIndex([]), pd.Series([], dtype=float)
    tmp = tmp.set_index('dt')
    if agg == 'hour':
        res = tmp['val'].resample('H').mean()
    else:
        res = tmp['val'].resample('D').mean()
    res = res.sort_index()
    return res.index, res


def top_n_picos(series: pd.Series, n: int = 5) -> List[Dict[str, Any]]:
    if series.empty:
        return []
    s = series.dropna()
    top = s.sort_values(ascending=False).head(n)
    items = []
    for ts, val in top.items():
        try:
            ts_out = ts.to_pydatetime().strftime(DATE_OUT_FMT)
        except Exception:
            ts_out = None
        items.append({'timestamp': ts_out, 'valor': float(val)})
    return items


def analyze_records_demanda(records: Union[List[Any], Dict[str, Any], None],
                            field_name: Optional[str] = 'potencia_ativa_tot',
                            agg: str = 'hour',
                            tol_perc: float = 0.10,
                            z_thr: float = 3.0) -> Dict[str, Any]:
    """
    Analisa o perfil de demanda.
    field_name: coluna a usar como demanda (p.ex. 'potencia_ativa_tot'); se None, tenta soma das fases.
    agg: 'hour' ou 'day' para granularidade agregada.
    """
    rows = _normalizar_input(records)
    df = pd.DataFrame(rows)

    if 'data_inc' not in df.columns:
        df['data_inc'] = None

    df['data_inc_dt'] = parse_datetime_series(df, 'data_inc')
    df = df.sort_values('data_inc_dt').reset_index(drop=True)

    # converter colunas numéricas
    for c in df.columns:
        if c not in ['data_inc', 'data_inc_dt', 'id_consumidor', 'id_equipamento']:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    chosen = _choose_demand_field(df, field_name)
    demanda_series = _compute_demand_series(df, chosen)

    # estatísticas básicas sobre demanda bruta (linha-a-linha)
    s = demanda_series.dropna()
    estatisticas_demanda = {
        'média': float(s.mean()) if not s.empty else None,
        'desvio_padrao': float(s.std()) if not s.empty else None,
        'mín': float(s.min()) if not s.empty else None,
        'máx': float(s.max()) if not s.empty else None,
        'amostras': int(s.count())
    }

    # agregação por hora/dia
    index_agg, series_agg = aggregate_time_series(df, 'data_inc_dt', demanda_series, agg=agg)

    # criar time_series_agg com timestamps formatados e valores
    time_series_agg = []
    for ts, val in series_agg.items():
        try:
            ts_out = ts.to_pydatetime().strftime(DATE_OUT_FMT)
        except Exception:
            ts_out = None
        time_series_agg.append({'timestamp': ts_out, 'valor': None if pd.isna(val) else float(val)})

    # perfis: média por hora do dia e por dia da semana (usando dados brutos)
    perfil_horario = []
    if not df['data_inc_dt'].isna().all():
        tmp = pd.DataFrame({'dt': df['data_inc_dt'], 'val': demanda_series})
        tmp = tmp.dropna(subset=['dt', 'val'])
        if not tmp.empty:
            tmp['hour'] = tmp['dt'].dt.hour
            hourly = tmp.groupby('hour')['val'].mean().reindex(range(24), fill_value=np.nan)
            perfil_horario = [{'hour': int(h), 'media': None if pd.isna(v) else float(v)} for h, v in hourly.items()]

    perfil_diario = []
    if not df['data_inc_dt'].isna().all():
        tmp2 = pd.DataFrame({'dt': df['data_inc_dt'], 'val': demanda_series})
        tmp2 = tmp2.dropna(subset=['dt', 'val'])
        if not tmp2.empty:
            # dayofweek: 0=segunda ... 6=domingo (pandas)
            daily = tmp2.groupby(tmp2['dt'].dt.dayofweek)['val'].mean().reindex(range(7), fill_value=np.nan)
            perfil_diario = [{'dayofweek': int(d), 'media': None if pd.isna(v) else float(v)} for d, v in daily.items()]

    # picos
    picos = top_n_picos(series_agg, n=5)

    # tendência da série agregada
    tendencias = {'agregada': tendencia_linear_simples(pd.Series(index_agg), series_agg)}

    # anomalias na série agregada
    anomalias = detectar_anomalias_zscore(series_agg, z_thr=z_thr)

    # eventos out of limit: compara com mediana da série agregada (se não vazia)
    events = []
    if not series_agg.dropna().empty:
        nominal = float(series_agg.median())
        low = nominal * (1 - tol_perc)
        high = nominal * (1 + tol_perc)
        for ts, val in series_agg.items():
            if pd.isna(val):
                continue
            if val < low:
                try:
                    ts_out = ts.to_pydatetime().strftime(DATE_OUT_FMT)
                except Exception:
                    ts_out = None
                events.append({'timestamp': ts_out, 'valor': float(val), 'tipo': 'queda_demanda'})
            elif val > high:
                try:
                    ts_out = ts.to_pydatetime().strftime(DATE_OUT_FMT)
                except Exception:
                    ts_out = None
                events.append({'timestamp': ts_out, 'valor': float(val), 'tipo': 'pico_demanda'})

    result = {
        'meta': {
            'registros': int(df.shape[0]),
            'periodo_inicio': df['data_inc_dt'].min().strftime(DATE_OUT_FMT) if not pd.isna(df['data_inc_dt'].min()) else None,
            'periodo_fim': df['data_inc_dt'].max().strftime(DATE_OUT_FMT) if not pd.isna(df['data_inc_dt'].max()) else None,
            'field_used': chosen if isinstance(chosen, str) else ('soma_fases' if chosen is None else None),
            'tolerancia_perc': tol_perc,
            'timestamps_parsed': int(df['data_inc_dt'].notna().sum()),
            'timestamps_invalid': int(df.shape[0] - df['data_inc_dt'].notna().sum())
        },
        'estatisticas_demanda': estatisticas_demanda,
        'perfil_horario': perfil_horario,
        'perfil_diario': perfil_diario,
        'time_series_agg': {'data': time_series_agg},
        'picos': picos,
        'tendencias_lin': tendencias,
        'anomalias_indices': anomalias,
        'events_out_of_limit': events
    }

    return _to_native(result)


def analisar_demanda_json(records: Union[List[Any], Dict[str, Any], None],
                          field_name: Optional[str] = 'potencia_ativa_tot',
                          agg: str = 'hour',
                          tol_perc: float = 0.10,
                          z_thr: float = 3.0) -> Dict[str, Any]:
    return analyze_records_demanda(records, field_name=field_name, agg=agg, tol_perc=tol_perc, z_thr=z_thr)
