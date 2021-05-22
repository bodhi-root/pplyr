# -*- coding: utf-8 -*-

# NOTE: Internally we define all functions with a leading underscore
#       for consistency.  Renaming them here allows us to override
#       reserved keywords like 'slice' and 'filter'.  The intended
#       use is for someone to import the entire module, so they would
#       import the whole module with 'import pplyr as pp' and then use
#       'pp.filter' and 'pp.slice' without having high-level conflicts.

from .pipeline import pipeline

from .groups import (
    _group_walk as group_walk,
    _group_map as group_map,
    _group_modify as group_modify
)

from .merge import (
    _inner_join as inner_join,
    _outer_join as outer_join,
    _left_join as left_join,
    _right_join as right_join
)

from .verbs import (
    _select as select,
    _drop as drop,
    _rename as rename,
    _filter as filter,
    _slice as slice,
    _slice_head as slice_head,
    _slice_tail as slice_tail,
    _slice_head as head,
    _slice_tail as tail,
    _slice_sample as slice_sample,
    _slice_min as slice_min,
    _slice_max as slice_max,
    _arrange as arrange,
    _mutate as mutate,
    _transmute as transmute,
    _summarise as summarise,
    _summarise as summarize,
    _ungroup as ungroup,
    _group_by as group_by,
    _group_by as groupby,
    _distinct as distinct,
    _tally as tally
)
    