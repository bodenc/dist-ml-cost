# coding: utf-8
# pylint: disable = C0103
"""Compatibility"""
from __future__ import absolute_import

import inspect
import sys

import numpy as np

is_py3 = (sys.version_info[0] == 3)

"""compatibility between python2 and python3"""
if is_py3:
    string_type = str
    numeric_types = (int, float, bool)
    integer_types = (int, )
    range_ = range

    def argc_(func):
        """return number of arguments of a function"""
        return len(inspect.signature(func).parameters)
else:
    string_type = basestring
    numeric_types = (int, long, float, bool)
    integer_types = (int, long)
    range_ = xrange

    def argc_(func):
        """return number of arguments of a function"""
        return len(inspect.getargspec(func).args)

"""json"""
try:
    import simplejson as json
except (ImportError, SyntaxError):
    # simplejson does not support Python 3.2, it throws a SyntaxError
    # because of u'...' Unicode literals.
    import json


def json_default_with_numpy(obj):
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    else:
        return obj


"""pandas"""
try:
    from pandas import Series, DataFrame
except ImportError:
    class Series(object):
        pass

    class DataFrame(object):
        pass

"""sklearn"""
try:
    from sklearn.base import BaseEstimator
    from sklearn.base import RegressorMixin, ClassifierMixin
    from sklearn.preprocessing import LabelEncoder
    from sklearn.utils import deprecated
    from sklearn.utils.multiclass import check_classification_targets
    from sklearn.utils.validation import check_X_y, check_array, check_consistent_length
    try:
        from sklearn.model_selection import StratifiedKFold, GroupKFold
        from sklearn.exceptions import NotFittedError
    except ImportError:
        from sklearn.cross_validation import StratifiedKFold, GroupKFold
        from sklearn.utils.validation import NotFittedError
    SKLEARN_INSTALLED = True
    _LGBMModelBase = BaseEstimator
    _LGBMRegressorBase = RegressorMixin
    _LGBMClassifierBase = ClassifierMixin
    _LGBMLabelEncoder = LabelEncoder
    LGBMDeprecated = deprecated
    LGBMNotFittedError = NotFittedError
    _LGBMStratifiedKFold = StratifiedKFold
    _LGBMGroupKFold = GroupKFold
    _LGBMCheckXY = check_X_y
    _LGBMCheckArray = check_array
    _LGBMCheckConsistentLength = check_consistent_length
    _LGBMCheckClassificationTargets = check_classification_targets
except ImportError:
    SKLEARN_INSTALLED = False
    _LGBMModelBase = object
    _LGBMClassifierBase = object
    _LGBMRegressorBase = object
    _LGBMLabelEncoder = None
#   LGBMDeprecated = None  Don't uncomment it because it causes error without installed sklearn
    LGBMNotFittedError = ValueError
    _LGBMStratifiedKFold = None
    _LGBMGroupKFold = None
    _LGBMCheckXY = None
    _LGBMCheckArray = None
    _LGBMCheckConsistentLength = None
    _LGBMCheckClassificationTargets = None
