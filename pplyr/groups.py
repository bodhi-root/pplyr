# -*- coding: utf-8 -*-

def _group_walk(dfg, func):
    """Applies 'func' to each group in dfg.  This is applied only for its
    side-effects.  The original dfg object is returned unchanged.
    """
    for key, df in dfg:
        func(df)
        
    return dfg

def _group_map(dfg, func):
    """Applies 'func' to each group in dfg.  The results are returned in
    a list.
    """
    output = []
    for key, df in dfg:
        output.append(func(df))
    return output

def _group_modify(dfg, func):
    """Applies 'func' to each group in dfg.  The result of each call should
    be a DataFrame that can be joined together in the end.
    """
    return dfg.apply(func)