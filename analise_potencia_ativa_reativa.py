# analise_potencia_evolucao.py
"""
Analisador da evolução da Potência Ativa e Reativa.

Exporta a função:
    analisar_potencia_json(records, tol_perc=0.10, z_thr=3.0)

Entrada aceita:
- lista de {"dadoEnergia": {...}}
- lista de {...} (inner objects)
- um único {...} (inner object)

Saída (apenas dados derivados relacionados à potência):
- meta (registros, período, nível nominal detectado para potência ativa total se aplicável, tolerância, contagem de timestamps)
- tabela_estatisticas (média/std/min/max/amostras por componente de potência)
- grafico_potencia_time_series (série temporal com potências por registro)
- tendencias_lin (slope + intercept por componente)
- anomalias_indices (índices via z-score por componente)
- events_out_of_limit (eventos quando potência ativa total sair de tolerância relativa ao nível nominal detectado)
"""
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

DATE_OUT_FMT = "%d/%m/%Y %H:%M"  # formato de saída requisitado pelo usuário


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


def estatisticas_por_componente(df: pd.DataFrame, componentes: List[str]) -> List[Dict[str, Any]]:
    tabela = []
    for c in componentes:
        if c not in df.columns:
            tabela.append({'Componente': c, 'Média': None, 'Desvio Padrão': None,
                           'Mín': None, 'Máx': None, 'Amostras': 0})
            continue
        s = df[c].dropna()
        if s.empty:
            tabela.append({'Componente': c, 'Média': None, 'Desvio Padrão': None,
                           'Mín': None, 'Máx': None, 'Amostras': 0})
            continue
        tabela.append({
            'Componente': c,
            'Média': float(s.mean()),
            'Desvio Padrão': float(s.std()),
            'Mín': float(s.min()),
            'Máx': float(s.max()),
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


def escolher_nivel_nominal_potencia_total(df: pd.DataFrame) -> Optional[float]:
    """
    Detecta um nível nominal para potencia_ativa_tot usando mediana das amostras,
    caso a coluna exista. Retorna None se não houver amostras válidas.
    """
    if 'potencia_ativa_tot' not in df.columns:
        return None
    s = df['potencia_ativa_tot'].dropna()
    if s.empty:
        return None
    return float(s.median())


def outside_limits_power(val: Optional[float], nominal: float, tol_perc: float) -> Optional[str]:
    if val is None or pd.isna(val):
        return None
    low = nominal * (1 - tol_perc)
    high = nominal * (1 + tol_perc)
    if val < low:
        return 'queda_potencia'
    if val > high:
        return 'sobrecarga_potencia'
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


def analyze_records_potencia(records: Union[List[Any], Dict[str, Any], None],
                             tol_perc: float = 0.10,
                             z_thr: float = 3.0) -> Dict[str, Any]:
    """
    Produz resultados derivados relacionados à evolução da potência ativa e reativa.
    """
    rows = _normalizar_input(records)
    df = pd.DataFrame(rows)

    # coluna de data padrão 'data_inc'
    if 'data_inc' not in df.columns:
        df['data_inc'] = None

    # parse robusto de datas
    df['data_inc_dt'] = parse_datetime_series(df, 'data_inc')
    df = df.sort_values('data_inc_dt').reset_index(drop=True)

    # converter colunas numéricas para facilitar cálculos
    for c in df.columns:
        if c not in ['data_inc', 'data_inc_dt', 'id_consumidor', 'id_equipamento']:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    # componentes de interesse (tanto por fase quanto totais, se presentes)
    componentes = []
    # potências ativas por fase
    componentes += [c for c in ['potencia_ativa_1', 'potencia_ativa_2', 'potencia_ativa_3'] if c in df.columns]
    # potência ativa total
    if 'potencia_ativa_tot' in df.columns:
        componentes.append('potencia_ativa_tot')
    # potências reativas por fase
    componentes += [c for c in ['potencia_reat_1', 'potencia_reat_2', 'potencia_reat_3'] if c in df.columns]
    # potência reativa total
    if 'potencia_reat_tot' in df.columns:
        componentes.append('potencia_reat_tot')

    # estatísticas por componente
    tabela = estatisticas_por_componente(df, componentes)

    # detectar nível nominal para potencia ativa total (se possível)
    nivel_nominal_ativa_tot = escolher_nivel_nominal_potencia_total(df)

    # tendências por componente
    tendencias = {}
    for c in componentes:
        series = df.get(c, pd.Series(dtype=float))
        tendencias[c] = tendencia_linear_simples(df['data_inc_dt'], series)

    # série temporal resumida (cada ponto é um registro analisado — apenas componentes)
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
        ponto = {'data_inc': dt_out}
        for c in componentes:
            ponto[c] = None if pd.isna(r.get(c)) else float(r.get(c))
        grafico.append(ponto)

    # eventos de queda/sobrecarga na potencia ativa total (quando nivel_nominal_ativa_tot detectado)
    events = []
    if nivel_nominal_ativa_tot is not None:
        for _, r in df.iterrows():
            dt = r['data_inc_dt']
            if pd.isna(dt):
                dt_out = None
            else:
                try:
                    dt_out = dt.strftime(DATE_OUT_FMT)
                except Exception:
                    dt_out = None
            val = r.get('potencia_ativa_tot') if 'potencia_ativa_tot' in r.index else None
            status = outside_limits_power(val, nivel_nominal_ativa_tot, tol_perc)
            if status in ('queda_potencia', 'sobrecarga_potencia'):
                events.append({
                    'data_inc': dt_out,
                    'componente': 'potencia_ativa_tot',
                    'valor': None if pd.isna(val) else float(val),
                    'tipo': status
                })

    # anomalias por componente (z-score)
    anomalias = {c: detectar_anomalias_zscore(df.get(c, pd.Series(dtype=float)), z_thr=z_thr) for c in componentes}

    result = {
        'meta': {
            'registros': int(df.shape[0]),
            'periodo_inicio': df['data_inc_dt'].min().strftime(DATE_OUT_FMT) if not pd.isna(df['data_inc_dt'].min()) else None,
            'periodo_fim': df['data_inc_dt'].max().strftime(DATE_OUT_FMT) if not pd.isna(df['data_inc_dt'].max()) else None,
            'nivel_nominal_ativa_tot': nivel_nominal_ativa_tot,
            'tolerancia_perc': tol_perc,
            'timestamps_parsed': int(df['data_inc_dt'].notna().sum()),
            'timestamps_invalid': int(df.shape[0] - df['data_inc_dt'].notna().sum())
        },
        'tabela_estatisticas': {'data': tabela},
        'grafico_potencia_time_series': {'data': grafico},
        'tendencias_lin': tendencias,
        'anomalias_indices': anomalias,
        'events_out_of_limit': events
    }

    return _to_native(result)


def analisar_potencia_json(records: Union[List[Any], Dict[str, Any], None],
                           tol_perc: float = 0.10,
                           z_thr: float = 3.0) -> Dict[str, Any]:
    """
    Função pública: chame com a lista de registros ou com um único registro.
    Retorna somente tipos nativos Python, prontos para json.dumps/Flask.
    """
    return analyze_records_potencia(records, tol_perc=tol_perc, z_thr=z_thr)
