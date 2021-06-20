# -*- coding: utf-8 -*-
"""
Created on Sun Dec 13 08:36:22 2020

@author: nicol
"""
from IPython.display import display
from tqdm.notebook import tqdm


verbose = True
suppress_warnings = list()
dh = None


class AutoPbars:

    def __init__(self):
        self.last = 0

    def __call__(self, iterator, **tqdmargs):
        if self.last > 0:
            tqdmargs['leave'] = False
        tqdmargs['position'] = self.last
        pbar = tqdm(iterator, **tqdmargs)
        self.last += 1
        for item in pbar:
            yield(item)
        self.last -= 1

    def pbar(self, **tqdmargs):
        if self.last > 0:
            tqdmargs['leave'] = False
        tqdmargs['position'] = self.last
        self.last += 1
        return tqdm(**tqdmargs)

    def close_pbar(self, pbar):
        self.last -= 1
        pbar.close()


_pbar = AutoPbars()


def set_verbose(value: bool):
    global verbose
    verbose = value


def pbar(iterable, **kwargs):
    if verbose:
        return _pbar(iterable, **kwargs)
    else:
        return iterable


def manual_pbar(**kwargs):
    if verbose:
        return _pbar.pbar(**kwargs)
    else:
        return None


def close_manual_pbar(pbar):
    if verbose:
        return _pbar.close_pbar(pbar)
    else:
        return None


def new_display():
    global dh
    if verbose:
        dh = display('', display_id=True)


def msg(string):
    if verbose:
        dh.update(string)


def computing_measure(measure, n):
    if verbose:
        string = "Computing {} for {}-grams...".format(measure, n)
        dh.update(string)


def done():
    if verbose:
        dh.update('Done!')


def reset():
    if verbose:
        dh.update('')
