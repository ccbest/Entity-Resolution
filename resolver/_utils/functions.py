
from typing import Any, Dict, Generator, Hashable, List


def merge_union(
        obj1: Any,
        obj2: Any,
        default_union_type: type = list,
        _key_path: List[str] = None
) -> Dict[str, Any]:
    """
    Merges two complex structures. Leaf values will be unioned instead of overwritten.
    The resulting union (as a list or set) will respect the original type of the
    leaf values, or will defer to the optional default_union_type parameter if
    neither are mutable collections.

    Does not mutate the original objects.

    Leaf values from the obj1 parameter will be given order priority (see fourth example).

    Examples:
        # Basic merge example
        obj1 = {"root_key": {"nested_key": "value1"}}
        obj2 = {"root_key": {"alternate_key": "value2"}}
        Returns: {"root_key": {"nested_key": "value1", "alternate_key": "value2":}}

        # Neither leaf value is a collection, so defers to default_union_type
        obj1 = {"root_key": {"nested_key": "value1"}}
        obj2 = {"root_key": {"nested_key": 2}}
        Returns: {"root_key": {"nested_key": ["value1", 2]}}

        # Leaf value from obj1 is a set, so a set is returned
        obj1 = {"root_key": {"nested_key": "value1"}}
        obj2 = {"root_key": {"nested_key": {"value1", "value2"}}}
        Returns: {"root_key": {"nested_key": {"value1", "value2"}}

        # Leaf value from obj2 is a list, but obj1 still maintains order priority
        obj1 = {"root_key": {"nested_key": "value1"}}
        obj2 = {"root_key": {"nested_key": ["value2", "value2"]}}
        Returns: {"root_key": {"nested_key": ["value1", "value2", "value3"]}}

        # Raises KeyError - obj1's leaf value is not a leaf in obj2
        obj1 = {"root_key": {"nested_key": "value1"}}
        obj2 = {"root_key": {"nested_key": {"deeply_nested_key": ["value2", "value3"]}}}
        Returns: KeyError

        # Raises ValueError - obj1 and obj2's leaf values (list and set, respectively) are
        # incapable of unioning
        obj1 = {"root_key": {"nested_key": {"value1"}}}
        obj2 = {"root_key": {"nested_key": ["value2", "value3"]}}
        Returns: ValueError

    Args:
        obj1 (Any): first complex structure
        obj2 (Any): second complex structure

        (Optionals)
        default_union_type (type): (Default list) Fives the option of specifying how to union two
                                   leaf values
        _key_path (Private): (Default None) Used to keep track of recursion location in the event
                             an error is raised.

    Returns:
        (Any) A complex data structure comprised of all kes from both A and B with their leaf values
        unioned.

    Raises:
        KeyError: If a leaf value is encountered in one of the complex structures, but its
                  counterpart is not a leaf.
        ValueError: If leaf values are of two different Collection types (e.g. one is a set and the
                    other is a list).
        ValueError: If default_union_type is defined as anything other than list or set
    """

    if (not obj1) or (not obj2):
        return obj1 or obj2

    _key_path = _key_path or []
    if default_union_type not in (list, set):
        raise ValueError("default_union_type must be list or set - "
                         f"you provided {default_union_type}")

    # If only one is a dict - KeyError
    if isinstance(obj1, dict) ^ isinstance(obj2, dict):
        raise KeyError(f"Key conflict at {'.'.join(_key_path) or 'root'}. Cannot merge a dict with "
                       f"a non-dict.")

    if isinstance(obj1, dict) and isinstance(obj2, dict):
        obj1, obj2 = obj1.copy(), obj2.copy()
        for key in obj2:
            if key in obj1:
                obj1.update(
                    {key: merge_union(
                        obj1[key],
                        obj2[key],
                        default_union_type=default_union_type,
                        _key_path=_key_path + [str(key)]
                    )}
                )
            else:
                obj1.update({key: obj2[key]})

        return obj1

    if isinstance(obj1, (list, set)) and isinstance(obj2, (list, set)):
        if not isinstance(obj1, type(obj2)):
            raise ValueError(f"Value conflict at {'.'.join(_key_path) or 'root'}. Cannot union"
                             f"a list and a set")

        return type(obj1)([*obj1, *obj2])

    if isinstance(obj1, (list, set)):
        return type(obj1)([*obj1, obj2])

    if isinstance(obj2, (list, set)):
        return type(obj2)([obj1, *obj2])

    return default_union_type([obj1, obj2])


def deduplicate_nested_structure(obj: Any) -> Any:
    """
    Deduplicates data structures within a deeply nested structure.

    Args:
        obj (Any): A deeply nested data structure.

    Returns:

    """

    if isinstance(obj, Generator):
        obj = list(obj)

    if isinstance(obj, (list, tuple)):
        if not isinstance(obj[0], Hashable):
            # Can't be deduplicated with set, so stringify first and then convert back
            return type(obj)(map(eval, set(map(str, obj))))

        return type(obj)(set(obj))

    if isinstance(obj, dict):
        return {k: deduplicate_nested_structure(v) for k, v in obj.items()}

    return obj

