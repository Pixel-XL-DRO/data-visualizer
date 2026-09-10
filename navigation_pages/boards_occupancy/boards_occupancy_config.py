import numpy as np

_FOUR = np.float64(4)

LAST_HOURS_AVAILABILITY = {
    "lubicz": {
        0: {20: _FOUR},
        1: {20: _FOUR},
        2: {20: _FOUR},
        3: {20: _FOUR},
        4: {21: _FOUR},
        5: {21: _FOUR},
        6: {20: _FOUR},
    },
    "struga": {
        0: {20: _FOUR},
        1: {20: _FOUR},
        2: {20: _FOUR},
        3: {20: _FOUR},
        4: {20: _FOUR},
        5: {20: _FOUR},
        6: {19: _FOUR},
    },
    "swietego-marcina": {
        0: {21: _FOUR},
        1: {21: _FOUR},
        2: {21: _FOUR},
        3: {21: _FOUR},
        4: {22: _FOUR},
        5: {22: _FOUR},
        6: {21: _FOUR},
    },
    "sokolska": {
        0: {20: _FOUR},
        1: {20: _FOUR},
        2: {20: _FOUR},
        3: {20: _FOUR},
        4: {21: _FOUR},
        5: {21: _FOUR},
        6: {19: _FOUR},
    },
    "grunwaldzka": {
        0: {21: _FOUR},
        1: {21: _FOUR},
        2: {21: _FOUR},
        3: {21: _FOUR},
        4: {21: _FOUR},
        5: {21: _FOUR},
        6: {21: _FOUR},
    },
    "kijowska": {
        0: {20: np.float64(5)},
        1: {20: np.float64(5)},
        2: {20: np.float64(5)},
        3: {20: np.float64(5)},
        4: {21: np.float64(5)},
        5: {21: np.float64(5)},
        6: {20: np.float64(5)},
    },
}
