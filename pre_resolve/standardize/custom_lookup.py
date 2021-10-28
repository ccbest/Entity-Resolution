
import csv
from pathlib import Path

from definitions import logger, ROOT_DIR


def _load_lookup_file(fname):
    """
    Loads conversion values into a dictionary.

    Args:
        fname (str): the file name. All conversion files must be located in:
                pre_resolve/standardize/resources

    Returns:

    """
    lookup = Path(ROOT_DIR, "pre_resolve", "standardize", "resources", fname)
    with open(lookup, 'r', encoding='utf-8-sig') as file:
        return {line[0]: line[1] for line in csv.reader(file)}


def _perform_lookup(entlet_rdd, field, filters, replacement_values):
    """
    Standardizes values in a given entlet field.

    Args:
        entlet_rdd: PySpark RDD with 2 columns - ( entlet_id , entlet )
        field (str): the column whose values will be updated
        filters (dict): Logical filters that need to be applied before standardization can occur,
                e.g. "country" must equal "US" before standardizing US state codes
                Format: { field_name : { "comparator": "" , "values": [] } }

        replacement_values (dict): conversion dictionary, where keys are the original
                                   values and values are the converted values

    Returns:

    """
    filtered = _apply_filters(entlet_rdd, filters)

    # Separate entlets that don't pass filter criteria - we'll join them back on at the end
    did_not_pass = filtered.filter(lambda x: x[2] is False)\
        .map(lambda x: (x[0], x[1]))
    passed = filtered.filter(lambda x: x[2] is True)

    # Pass the replacement values to the entlet as a dict
    # ( entlet_id , entlet )
    standardized = passed.map(lambda x: (x[0], x[1]._standardize_values(field, replacement_values)))

    return standardized


def _apply_filters(entlet_rdd, filters):
    """
    If filters are applied to the standardize logic,
    Args:
        entlet_rdd:
        filters:

    Returns:

    """
    if not filters:
        return entlet_rdd.map(lambda x: (*x, True))

    filter_response = entlet_rdd.map(lambda x: (*x, x[1].check_values_exist(filters)))

    return filter_response


def us_state_to_2code(entlet_rdd, field, filters):
    """
    Standardizes names of US states.

    Args:
        entlet_rdd (RDD):
        field: name of the column representing US states
        filters:

    Returns:

    """
    std_values = _load_lookup_file("us_state_two_code.csv")

    for std_filter in filters:
        entlet_rdd = entlet_rdd.apply(
            lambda x: x[0].standardize_values(
                field,
                std_filter,
                lambda y: std_values.get(y.upper(), y)
            )
        )

    return entlet_rdd

"""
DEBUG:
filters = {'country': {'comparator': '==', 'values': ['US']}}

"""