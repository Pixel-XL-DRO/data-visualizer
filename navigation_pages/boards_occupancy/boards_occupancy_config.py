import numpy as np

_FOUR = np.float64(4)

LAST_HOURS_AVAILABILITY = {
    "lubicz": {
        0: {20: _FOUR},
        1: {20: _FOUR},
        2: {20: _FOUR},
        3: {20: _FOUR},
        4: {22: _FOUR},
        5: {22: _FOUR},
        6: {20: _FOUR},
    },
    "swietego-marcina": {
        0: {22: _FOUR},
        1: {22: _FOUR},
        2: {22: _FOUR},
        3: {22: _FOUR},
        4: {23: _FOUR},
        5: {23: _FOUR},
        6: {21: _FOUR},
    },
    "sokolska": {
        0: {21: _FOUR},
        1: {21: _FOUR},
        2: {21: _FOUR},
        3: {21: _FOUR},
        4: {22: _FOUR},
        5: {22: _FOUR},
        6: {20: _FOUR},
    },
    "grunwaldzka": {
        0: {21: _FOUR},
        1: {21: _FOUR},
        2: {21: _FOUR},
        3: {21: _FOUR},
        4: {22: _FOUR},
        5: {22: _FOUR},
        6: {21: _FOUR},
    },
    "kijowska": {
        0: {21: np.float64(5)},
        1: {21: np.float64(5)},
        2: {21: np.float64(5)},
        3: {21: np.float64(5)},
        4: {23: np.float64(5)},
        5: {23: np.float64(5)},
        6: {21: np.float64(5)},
    },
}
