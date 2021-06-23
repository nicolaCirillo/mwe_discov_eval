import numpy as np


def pmi(f_1, f_2, f_12, N):
    """

    """
    p_1 = f_1 / N
    p_2 = f_2 / N
    p_12 = f_12 / N
    pmi = np.log2(p_12 / (p_1 * p_2))
    return pmi


def b_pmi(f_1, f_2, f_12, N):
    p_1 = f_1 / N
    p_2 = f_2 / N
    p_12 = f_12 / N
    b_pmi = np.log2((f_12 * p_12) / (p_1 * p_2))
    return b_pmi


def dice(f_1,  f_2, f_12, N=None):
    dice = 2*f_12 / (f_1 + f_2)
    return dice


def t(f_1,  f_2, f_12, N):
    p_1 = f_1 / N
    p_2 = f_2 / N
    p_12 = f_12 / N
    t = (p_12 - p_1*p_2) / np.sqrt(p_12 / N)
    return t


def ll(f_1, f_2, f_12, N):
    def logL(k, n, x):
        return np.log(x) * k + np.log(1-x) * (n-k)
    p = f_2 / N
    p_1 = min(f_12 / f_1, 0.99)
    p_2 = max(f_2 - f_12, 1) / (N - f_1)
    ll_ratio = logL(f_12, f_1, p) + logL(f_2 - f_12, N - f_1, p)\
        - logL(f_12, f_1, p_1) - logL(f_2 - f_12, N - f_1, p_2)
    return ll_ratio


def scp(f_1,  f_2, f_12, N):
    p_1 = f_1 / N
    p_2 = f_2 / N
    p_12 = f_12 / N
    scp = p_12**2 / (p_1 * p_2)
    return scp


def b_scp(f_1,  f_2, f_12, N):
    p_1 = f_1 / N
    p_2 = f_2 / N
    p_12 = f_12 / N
    b_scp = f_12 * p_12**2 / (p_1 * p_2)
    return b_scp
