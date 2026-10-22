"""Referencia em pandas das formulas da Silver/Gold.
Serve para conferir, com numeros que voce calcula na mao, o que o Spark entrega.
Rodar:  pip install pytest pandas  &&  pytest tests -q
"""
import math

import pandas as pd


def retorno_log(precos):
    s = pd.Series(precos, dtype="float64")
    return (s / s.shift(1)).apply(lambda x: math.log(x) if x == x else float("nan"))


def var_parametrico_95(vol_anual):
    return 1.645 * vol_anual / math.sqrt(252)


def drawdown_maximo(precos):
    s = pd.Series(precos, dtype="float64")
    return float(((s - s.cummax()) / s.cummax()).min())


def rsi(retornos, n=14):
    r = pd.Series(retornos, dtype="float64")
    ganho = r.clip(lower=0).rolling(n, min_periods=1).mean()
    perda = (-r.clip(upper=0)).rolling(n, min_periods=1).mean()
    rs = ganho / perda
    # mesma regra da Silver: sem perdas na janela, RSI = 100
    return (100 - 100 / (1 + rs)).where(perda != 0, 100.0)


def test_retorno_log_e_aditivo():
    precos = [100, 110, 99, 120]
    soma = retorno_log(precos).sum()
    assert math.isclose(soma, math.log(120 / 100), rel_tol=1e-12)


def test_drawdown_maximo():
    # pico 120 -> vale 90: queda de 25%
    assert math.isclose(drawdown_maximo([100, 120, 90, 110]), -0.25)


def test_var_95_um_dia():
    # volatilidade anual de 30% => diaria ~1,89% => VaR ~3,1%
    assert math.isclose(var_parametrico_95(0.30), 0.0310, abs_tol=5e-4)


def test_rsi_so_ganhos_vale_100():
    assert rsi([0.01, 0.02, 0.01, 0.03]).iloc[-1] == 100.0


def test_rsi_equilibrado_vale_50():
    assert math.isclose(rsi([0.01, -0.01, 0.01, -0.01], n=4).iloc[-1], 50.0)
