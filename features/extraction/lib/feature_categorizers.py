"""
Helper methods for categorizing features.

These methods categorize features from a large number of possible categories
in the database into a small number of useful categories.
"""


def race_from_string(str):
    """Convert race to one of ['white', 'black', None]."""
    race_dict = {
        "White/Caucasian": 'white',
        "Black/African American": 'black',
        "Unknown": None,
        "": None
    }
    return race_dict.get(str, 'other')


def marital_status_from_string(str):
    """Convert marital status to one of ['single', 'partner', 'married', 'separated', 'widowed']."""
    marital_status_dict = {
        'Single': 'single',
        'Significant other': 'partner',
        'Life Partner': 'partner',
        'Married': 'married',
        'Divorced': 'separated',
        'Legally Separated': 'separated',
        'Separated': 'separated',
        'Widowed': 'widowed'
    }
    return marital_status_dict.get(str, 'other')
