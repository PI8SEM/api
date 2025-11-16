"""
analise_tensao_rms.py

Versão enxuta: apenas funções importáveis para analisar os registros.
- Exporte a função `analisar_dados_json(records, tol_perc=0.10, z_thr=3.0)`.
- Não contém CLI, leitura/escrita de arquivos nem código desnecessário.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd


DATE_FMT = "%d/%m/%Y %H:%M:%S"
POSSIVEIS_NIVEIS = [110.0, 220.0, 380.0]


def parse_dt(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    try:
        return datetime.strptime(s, DATE_FMT)
    except Exception:
        return None


def escolher_nivel_nominal(df: pd.DataFrame, fases=('tensao_1','tensao_2','tensao_3')) -> Optional[float]:
    vals = []
    for f in fases:
        if f in df.columns:
            vals.extend(df[f].dropna().tolist())
    if not vals:
        return None
    med = float(pd.Series(vals).median())
    escolhido = min(POSSIVEIS_NIVEIS, key=lambda x: abs(x - med))
    return float(escolhido)


def estatisticas_por_fase(df: pd.DataFrame, fases=('tensao_1','tensao_2','tensao_3')) -> List[Dict[str, Any]]:
    tabela = []
    for f in fases:
        if f not in df.columns:
            tabela.append({'Fase': f, 'Média (V)': None, 'Desvio Padrão (V)': None, 'Mín (V)': None, 'Máx (V)': None, 'Amostras': 0})
            continue
        s = df[f].dropna()
        if s.empty:
            tabela.append({'Fase': f, 'Média (V)': None, 'Desvio Padrão (V)': None, 'Mín (V)': None, 'Máx (V)': None, 'Amostras': 0})
            continue
        tabela.append({
            'Fase': f,
            'Média (V)': float(s.mean()),
            'Desvio Padrão (V)': float(s.std()),
            'Mín (V)': float(s.min()),
            'Máx (V)': float(s.max()),
            'Amostras': int(s.count())
        })
    return tabela


def tendencia_linear_simples(times: pd.Series, series: pd.Series) -> Dict[str, Optional[float]]:
    mask = (~times.isna()) & (~series.isna())
    if mask.sum() < 2:
        return {'slope_per_s': None, 'intercept': None}
    xs = (times[mask] - times[mask].min()).dt.total_seconds().astype(float)
    ys = series[mask].astype(float)
    coeffs = np.polyfit(xs, ys, 1)
    return {'slope_per_s': float(coeffs[0]), 'intercept': float(coeffs[1])}


def calcula_unbalance(df: pd.DataFrame, fases=('tensao_1','tensao_2','tensao_3')) -> pd.Series:
    def row_unb(r):
        vals = [r[f] for f in fases if f in r.index and not pd.isna(r[f])]
        if len(vals) < 2:
            return np.nan
        avg = float(np.mean(vals))
        if avg == 0:
            return np.nan
        return float(max(abs(v - avg) for v in vals) / avg * 100.0)
    return df.apply(row_unb, axis=1)


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


def outside_limits(val: float, nominal: float, tol_perc: float) -> Optional[str]:
    if val is None or pd.isna(val):
        return None
    low = nominal * (1 - tol_perc)
    high = nominal * (1 + tol_perc)
    if val < low:
        return 'subtensão'
    if val > high:
        return 'sobretensão'
    return 'ok'


def analyze_records(records: List[Dict[str, Any]], tol_perc: float = 0.10, z_thr: float = 3.0) -> Dict[str, Any]:
    rows = [r.get('dadoEnergia', {}) if isinstance(r, dict) else r for r in records]
    df = pd.DataFrame(rows)

    df['data_coleta_dt'] = df['data_coleta'].apply(parse_dt)
    df = df.sort_values('data_coleta_dt').reset_index(drop=True)

    for c in df.columns:
        if c not in ['data_coleta', 'data_coleta_dt', 'id_consumidor', 'id_equipamento']:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    fases = ['tensao_1', 'tensao_2', 'tensao_3']

    nivel_detectado = escolher_nivel_nominal(df, fases)

    tabela = estatisticas_por_fase(df, fases)

    tendencias = {}
    for f in fases:
        tendencias[f] = tendencia_linear_simples(df['data_coleta_dt'], df[f])

    df['unbalance_perc'] = calcula_unbalance(df, fases)

    grafico = []
    for _, r in df.iterrows():
        grafico.append({
            'data_coleta': r['data_coleta_dt'].strftime(DATE_FMT) if not pd.isna(r['data_coleta_dt']) else None,
            'tensao_1': None if pd.isna(r.get('tensao_1')) else float(r.get('tensao_1')),
            'tensao_2': None if pd.isna(r.get('tensao_2')) else float(r.get('tensao_2')),
            'tensao_3': None if pd.isna(r.get('tensao_3')) else float(r.get('tensao_3')),
            'potencia_ativa_tot': None if pd.isna(r.get('potencia_ativa_tot')) else float(r.get('potencia_ativa_tot')),
            'unbalance_perc': None if pd.isna(r.get('unbalance_perc')) else float(r.get('unbalance_perc'))
        })

    events = []
    if nivel_detectado is not None:
        for _, r in df.iterrows():
            for f in fases:
                status = outside_limits(r.get(f), nivel_detectado, tol_perc)
                if status in ('subtensão', 'sobretensão'):
                    events.append({
                        'data_coleta': r['data_coleta_dt'].strftime(DATE_FMT) if not pd.isna(r['data_coleta_dt']) else None,
                        'fase': f,
                        'valor': None if pd.isna(r.get(f)) else float(r.get(f)),
                        'tipo': status
                    })

    anomalias = {f: detectar_anomalias_zscore(df[f], z_thr=z_thr) for f in fases}

    corr_cols = [c for c in ['tensao_1','tensao_2','tensao_3','potencia_ativa_tot','fator_potencia'] if c in df.columns]
    correl = df[corr_cols].corr().to_dict() if len(corr_cols) >= 2 else {}

    result = {
        'meta': {
            'registros': int(df.shape[0]),
            'periodo_inicio': df['data_coleta_dt'].min().strftime(DATE_FMT) if not pd.isna(df['data_coleta_dt'].min()) else None,
            'periodo_fim': df['data_coleta_dt'].max().strftime(DATE_FMT) if not pd.isna(df['data_coleta_dt'].max()) else None,
            'nivel_nominal_detectado': nivel_detectado,
            'tolerancia_perc': tol_perc
        },
        'tabela_estatisticas': {'data': tabela},
        'grafico_tensao_time_series': {'data': grafico},
        'tendencias_lin': tendencias,
        'anomalias_indices': anomalias,
        'events_out_of_limit': events,
        'correlacoes': correl
    }

    return result


def analisar_dados_json(records: List[Dict[str, Any]], tol_perc: float = 0.10, z_thr: float = 3.0) -> Dict[str, Any]:
    """Função exportada: chame diretamente com a lista de registros (JSON parseado)."""
    return analyze_records(records, tol_perc=tol_perc, z_thr=z_thr)
