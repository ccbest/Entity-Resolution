
from __future__ import annotations
from collections import defaultdict
from copy import deepcopy
from hashlib import md5
import itertools
import json
import socket
from typing import Any, Callable, Dict, Generator, List, Tuple, Union

from definitions import config

# TODO: Values should have their type stored in the class for checking to avoid collisions
# TODO: Deduplication of values could be moved to post-munge for efficiency


class Entlet(object):
    """
    This class lets you build up information about an entlet and then output it into flattened fragments.

    NOTE: the following fields are reserved
      - ent_type
      - entlet_id

    """

    REQUIRED_FIELDS = ['ent_type', 'entlet_id', 'data_source']
    ER_FIELDS = {}
    UID_FIELDS = ['ent_type', 'data_source']

    CUSTOM_UID_FIELDS = None
    SOURCE_UID_FIELD = None

    STREAMING_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    STREAMING_CONNECTION = None
    STREAMING_ADDR = None

    def __init__(self):
        self.products = ()
        self.values = defaultdict(list)
        self.defined_uid = None

        self.fields = set()
        self.const_values = {}

        if not self.ER_FIELDS:
            self.define_fragment_fields()

        # The RDD can't access the class, so this needs to be copied to the instance
        self.er_fields = self.ER_FIELDS

    def __getitem__(self, key: str):
        """
        Enables retrieval of values that were stored using the .add() method. Permits dot-delimited
        key notation.

        Reference ._dot_delimited_lookup for a more detailed explanation.

        Args:
            key (str): The key to look for in dot form, eg. "key1.key2"

        Returns:
            (Any) The value of the last provided key
        """
        split_key = key.split('.')
        if len(split_key) == 1:
            return self.values.get(split_key[0], self.const_values.get(split_key[0], ValueError))

        else:
            if split_key[0] in self.values:
                return [self.get_recursive(obj, split_key[1:]) for obj in self.values[split_key[0]]]
            elif split_key[0] in self.const_values:
                return [self.get_recursive(obj, split_key[1:]) for obj in self.const_values[split_key[0]]]
            else:
                raise ValueError(f"Key {split_key[0]} does not exist on the entlet.")

    def __repr__(self):
        """ So we can easily see in the debugger """
        attrs = ', '.join('{}={}'.format(k, v) for k, v in self.const_values.items())
        return f'Entlet({attrs})'

    def __contains__(self, item):
        """Boolean check if key exists in .values"""
        _ = self[item]
        return bool(_)

    def get_recursive(self, obj: Union[Dict, str], key_parts: List[str]):
        """
        Permits dot-delimited key retrieval from loaded values.

        The method used to store values is important to understand. Root values in self.values are converted
        to lists containing the values passed. To illustrate, consider the following:
        entlet.add({"test": "value"})
        entlet.add({"test": "value2"})
        entlet.values  ---> { "test": ["value", "value2" ] }

        Example:
            If the config were equal to:
            {
                "key1": {
                    "key2": "retrieved_value"
                }
            }

            ...You could retrieve "retrieved_value" by passing key "key1.key2"

        Args:
            obj (dict): The dictionary structure to retrieve a value from
            key_parts (list): The key to look for, in order of nesting

        Returns:
            (Any) The value of the last provided key
        """
        # Found the desired setting
        if len(key_parts) == 0 or not key_parts[0]:
            return obj

        # Look for the next part of the path
        top_level = key_parts.pop(0)
        if top_level not in obj:
            raise KeyError(f"Key not found: {top_level}")

        return self.get_recursive(obj[top_level], key_parts)

    @classmethod
    def define_custom_uid_fields(cls, *args):
        """
        Defines the fields that will be used to create a custom id.

        Before you use this, remember that the entlet ID includes the entity type and the source.
        If your data source provides some sort of not-very-unique ID like "123", then it should
        still be good enough to uniquely identify the entlet so long as the ID is unique within the
        data source itself.

        If you still decide to create custom uids with this method, the custom id will take all values
        stored in the specified fields, sort and deduplicate them, and then provide a hash of the
        stringified result. The IDs will only be stable between runs so long as the underlying information
        hasn't changed.

        Args:
            *args:

        Returns:
            (cls)
        """
        cls.CUSTOM_UID_FIELDS = sorted(args)
        cls.REQUIRED_FIELDS += sorted(args)
        return cls

    @classmethod
    def define_source_uid_field(cls, field: str):
        """
        Defines which field from a given source constitutes a unique id. This is the recommended way of
        creating the entlet unique ID.

        Once defined, a source_uid_field MUST contain a string as its corresponding value.

        Example:
        Imagine the field "state_code" (with value "NY") exists on the entlet (with ent_type "state") and is defined
        here as a unique id field from the source "us_state_codes". The resulting entlet_id would
        be: state:us_state_codes:NY

        Args:
            field: The name of the field that represents a source-provided unique ID.

        Returns:
            (cls)
        """
        cls.SOURCE_UID_FIELD = field
        cls.REQUIRED_FIELDS.append(field)
        return cls

    def define_individual_uid(self, uid: str):
        """
        Defines a value that will be attached to the entlet as its unique id (uid). This value will be used together with
        the other fields defined by UID_FIELDS to produce the entlet_id.

        Example:
        Imagine this entlet has ent_type "state" and is from the data source "us_state_codes". You call this method
        and pass "1" to the source_uid parameter. The resulting entlet_id would be: state:us_state_codes:1

        Args:
            uid (str): The unique ID you are passing

        Returns:

        """
        if self.defined_uid and self.defined_uid != uid:
            raise ValueError("Source UID has already been defined.")

        self.defined_uid = uid

    def _generate_entlet_id(self):
        """
        Will create a "unique" entlet id based on supplied settings and store the resulting ID in self.values
        under "entlet_id"

        Returns:
            (self)
        """

        uid_values = [self.values.get(field, None) for field in self.UID_FIELDS]
        if not all(uid_values):
            missing = [field for field in self.UID_FIELDS if not self.const_values[field]]
            raise ValueError(f"Entlet is missing the following required fields: {missing}")

        # Preferred method, so considered first
        if self.SOURCE_UID_FIELD:
            entlet_unique_id = self.values.get(self.SOURCE_UID_FIELD, ValueError)

        elif self.defined_uid:
            entlet_unique_id = [self.defined_uid]

        # Least preferred method, so considered last
        elif self.CUSTOM_UID_FIELDS:
            entlet_unique_id = [md5(
                str(set([self.values.get(value, None) for value in self.CUSTOM_UID_FIELDS]))
            )]

        else:
            raise ValueError("Unable to generate Entlet ID - no method specified.")

        uid = ':'.join(uid_values + entlet_unique_id)

        self.const_values["entlet_id"] = uid

        return self

    @classmethod
    def begin_streaming(cls) -> None:
        """
        Opens a connection to Spork Streaming.

        Returns:
            None
        """
        cls.STREAMING_SOCKET.bind(('0.0.0.0', config.get("spark_streaming_port", 9999)))
        cls.STREAMING_SOCKET.listen(1)
        cls.STREAMING_CONNECTION, cls.STREAMING_ADDR = cls.STREAMING_SOCKET.accept()

    @classmethod
    def _complete_source(cls) -> None:
        """
        Cleanup for when the data sources completes munging.

        Returns:

        """
        cls.STREAMING_CONNECTION.close()

    def submit(self) -> True:
        """
        Submits the entlet to the Spark Streaming pipeline, which will run the remaining
        pre-resolve stages.

        Returns:
            True
        """
        if not self.const_values.get("entlet_id", None):
            self._generate_entlet_id()

        to_be_sent = bytes(json.dumps(self.dump()) + "\n", 'utf-8')
        self.STREAMING_CONNECTION.sendall(to_be_sent)

        return True

    def add(self, obj: dict):
        """
        Add values to this entlet.

        Args:
            obj: keys and values

        Returns:
            None

        Examples:
            # Add single value to a field. The key will be used as the field name.
            entlet = Entlet()
            entlet.add({"a": 1, "b": 2})
            entlet.values
            {"a": [1], "b": [2]}

            # Add nested fields in order to retain associated between the values.
            entlet = Entlet()
            entlet.add({a: {"b": 1, "c": 2}})
            entlet.values
            {"name": [{"name": "Carl, "type": "first"}]}

            # Add multiple values in one call.
            entlet = Entlet()
            entlet.add({a: [1, 2], b: [{"b": 3, "c": 4}, {"b": 5, "c": 6}]})
            entlet.values
            {"a": [1, 2], "b": [{"b": 3, "c": 4}, {"b": 5, "c": 6}]}

            # Duplicates are dropped within each key. Associated fields only drop if all values are duplicate.
            entlet = Entlet()
            entlet.add({a: 1, b: 2, c: {"c": 2, "d": 3}})
            entlet.add({a: 1, b: 3, c: {"c": 2, "d": 4}})
            entlet.add({c: {"c": 2, "d": 3}})
            entlet.values
            {"a": [1], "b": [2, 3], "c": [{"c": 2, "d": 3}, {"c": 2, "d": 4}]}

            # Values must be of the same type per field or will throw an error
            entlet = Entlet()
            entlet.add({a: [1, 2, "string"]})   (ValueError)

            # All required fields must be added using this method. These fields must have one value per Entlet
            # and will be stored as strings.
            entlet = Entlet()
            entlet.add({entlet_id: "id123", ent_type: "entity"})

        """
        def merge_values(a, b, _path):
            if isinstance(b, list):
                for item in b:
                    merge_values(a, item, _path)
                    return True

            if isinstance(b, type(a[0])):
                if b not in a:
                    a.append(b)
                return True

            raise ValueError(f"Field {_path} expected type {type(a[0])}, received type {type(b)}")

        for key in obj:
            if key == "entlet_id":
                raise KeyError("You cannot use the .add() method to declare an entlet id.")

            if key in self.REQUIRED_FIELDS:
                self._set_const({key: obj[key]})
                continue

            if key not in self.values or not self.values[key]:
                if isinstance(obj[key], list):
                    self.values[key] += obj[key]

                else:
                    self.values[key].append(obj[key])
            else:
                merge_values(self.values[key], obj[key], key)

        return self

    def merge(self, obj):
        """
        Accepts the .dump() from another entlet. Should not be used outside of rollup.
        Functionally the same as .add() except doesn't check for constant fields

        Args:
            obj: keys and values

        Returns:
            None

        """
        def merge_values(a, b, _path):
            if isinstance(b, list):
                for item in b:
                    merge_values(a, item, _path)
                    return True

            if isinstance(b, type(a[0])):
                if b not in a:
                    a.append(b)
                return True

            raise ValueError(f"Field {_path} expected type {type(a[0])}, received type {type(b)}")

        for key in obj:
            if key not in self.values:
                if isinstance(obj[key], list):
                    self.values[key] += obj[key]

                elif isinstance(self.values[key], list):
                    self.values[key].append(obj[key])

                else:
                    self.values[key] = [self.values[key], obj[key]]

            else:
                merge_values(self.values[key], obj[key], key)

        return self

    def _set_const(self, obj):
        """
        Set constant values on the entlet. Ensures constant values don't get overwritten, which would indicate
        a problem with munge.

        Constant values are critical to producing an entlet_id, and so values must be strings.

        Called by .add() when one of the fields to be added appears in cls.REQUIRED_FIELDS

        Args:
            obj: Key/value pairs to set as consts
        """
        for field, value in obj.items():
            if self.const_values.get(field) and self.const_values.get(field) != value:
                raise KeyError(f"Required field {field} is already assigned a value.")

            # Must be stringified in order to use
            if not isinstance(value, str):
                raise ValueError("Field {field} requires values of type string - got {type(value)}.")

            self.const_values[field] = value

    def dump(self) -> Dict[str, Union[list, str]]:
        """
        Return all information stored on the entlet as a dictionary.

        Returns:
            Dict[str, Union[list, str]]
        """
        values = deepcopy(dict(self.values))
        values.update(self.const_values)
        return values

    def standardize_values(self,
                           standardization_field: str,
                           filter_ruleset: dict,
                           standardization_function: Callable
                           ) -> Entlet:
        """
        Substitutes values for their standardized counterparts. Values that are substituted get moved
        to a new '{field_name}_raw' field.

        Filters should follow the below data structure:
        {
            "field_name": the name of the field
            "comparator": comparator (from operators),
            "values": ...one or more values...
        }

        Args:
            standardization_field (str): The name of the field to be standardized
            filter_ruleset (dict): See above.
            standardization_function (Callable): The function by which to compare the value on the
                                                 entlet versus the value supplied by the filter_value
                                                 parameter.

        Returns:
            (self) The same entlet instance, with the standardization rule applied.
        """
        if standardization_field not in self:
            return self

        std_field_split = standardization_field.split(".")
        if len(std_field_split) > 1:
            standardization_nested_field = std_field_split[1:]
        else:
            standardization_nested_field = ['']

        passed_filter, did_not_pass_filter = self._apply_standardization_filters(
            standardization_field,
            filter_ruleset["field_name"],
            filter_ruleset["value"],
            filter_ruleset["comparator"]
        )

        # Non-nested values should be copied because they'll be modified during iteration, but
        # nested values need their references preserved
        for value in passed_filter.copy():
            if isinstance(value, dict):
                original_value = self.get_recursive(value, standardization_nested_field)
            else:
                original_value = value

            standardized_value = standardization_function(original_value)

            if original_value != standardized_value:

                # If the field is nested, we add a {field}_raw field on to the nested object
                if isinstance(value, dict):
                    parent_of_value = self.get_recursive(
                        value,
                        std_field_split[1:-1]
                    )
                    parent_of_value.update({
                        f"{std_field_split[-1]}_raw": original_value,
                        std_field_split[-1]: standardized_value
                    })
                    continue

                # If the field isn't nested, the old values get pushed to a new root field
                self[standardization_field].remove(original_value)
                self.add({
                    standardization_field: standardized_value,
                    f"{standardization_field}_raw": original_value
                })

        return self

    def _apply_standardization_filters(self,
                                       standardization_field: str,
                                       filter_field: str,
                                       filter_value: Any,
                                       filter_comparator: Callable
                                       ) -> Tuple[List, List]:
        """
        Applies filters that are defined by standardization configuration.

        Args:
            standardization_field (str):
            filter_field (str): The name of the field whose values will determine whether or not
                                the filter is passed
            filter_value (Any): The value to filter according to. For example, if the filter
                                criteria were >= 1, "1" would be the filter value.
            filter_comparator (Callable): The function by which to compare the value on the entlet
                                          versus the value supplied by the filter_value parameter.

        Returns:
            (Tuple[List, List]) A tuple with two elements. The first element is those values which
            passed the filter, the second is the ones which didn't.
        """
        filter_field_root_key = filter_field.split('.')[0]
        filter_nested_field = '.'.join(filter_field.split('.')[1:])

        # Root key for the standardization field and filter are the same - filters are locally
        # scoped such that individual nested objects must pass the filter
        if standardization_field.split('.')[0] == filter_field_root_key:
            passed_filter = []
            did_not_pass_filter = []
            for nested_obj in self[standardization_field.split('.')[0]]:
                if filter_comparator(
                        self.get_recursive(nested_obj, filter_nested_field.split('.')),
                        filter_value
                ):
                    passed_filter.append(nested_obj)

                else:
                    did_not_pass_filter.append(nested_obj)

            return passed_filter, did_not_pass_filter

        # Root key for the standardization field name and filter are different - filters are
        # globally scoped such that if any value in the filter field passes, all values in the
        # standardization field are eligible for standardization
        elif any([filter_comparator(
                    self.get_recursive(
                        filter_obj,
                        filter_nested_field.split('.')
                    ),
                    filter_value
                  ) for filter_obj in self[filter_field_root_key]]):
            return self.values[standardization_field], []

        # Entlet does not meet filter criteria
        return [], self.values[standardization_field]

    def clear(self) -> Entlet:
        """Removes all values stored on the entlet. Really should only be used in testing."""
        self.values.clear()
        self.const_values.clear()
        return self

    @classmethod
    def define_fragment_fields(cls) -> None:
        """
        Parses the ER configuration so we can understand what fields are needed by which strategies.
        Updates the Entlet class to reflect these requirements, stored as below:

        {
            strategy_name: [ field_names ]
        }

        Returns:
            None
        """

        er_fields = defaultdict(set)
        for strategy, details in config["strategies"].items():
            er_fields[strategy].update([a["field_name"] for a in details["similarity"]])
            er_fields[strategy].update([a["field_name"] for a in details["blocking"]])
            if 'filters' in details:
                er_fields[strategy].update([a["field_name"] for a in details["filters"]])

            if 'partition' in details:
                er_fields[strategy].update(details["partition"])

            er_fields[strategy].add("entlet_id")

        cls.ER_FIELDS = dict(er_fields)

    def _produce_fragment_product(self, obj: dict, field_names: set):
        """
        Produces a cartesian product of a dictionary based on its values. Empty values are
        effectively treated as multiplying by 1 rather than multiplying by 0.

        Example:
        { "key1": ["val1", "val2"], "key2": ["val1"] }
        ...fragments into...
        { "key1": "val1", "key2": "val1" }
        { "key1": "val2", "key2": "val1" }

        If nested values are present, associations between the values will be preserved and nested
        keys will be transformed into dot-delimited root keys.

        Example:
        {
            "country": [
                { "name": "United States", "type": "name" },
                { "name": "US", "name_type": "iso2" }
            ],
            "state": "NY"
        }
        ...fragments into...
        { "country.name": "United States", "country.type": "name", "state": "NY" }
        { "country.name": "US", "country.type": "iso2", "state": "NY" }

        Notice that no fragment is created where country.name is "United States" and country.type is
        "iso2", because those values don't exist together in the same nested object.

        Args:
            obj (dict): (values must be lists)
            field_names (set): the fields used in ER (formatted as "{field}.{subfield}"). Unused subfields
                              will be removed.

        Returns:
            (Generator[List[Dict]]) The fragments created.
        """
        nest_map = defaultdict(set)
        for field in field_names:
            nest_map[field.split('.')[0]].add('.'.join(field.split('.')[1:]))

        # every value of a root key (except for const values) will be a list, but putting an
        # empty list through itertools.product behave like multiplying by 0. Configuration may
        # allow for null values in an ER field, so have to separate and add back in later
        obj_non_empty = {key: value for key, value in obj.items() if value}
        obj_empty = {key: '' for key, value in obj.items() if not value}

        for field, value in obj_non_empty.items():
            if isinstance(value[0], dict):
                # dicts can't be hashed, so dump them to a json string
                obj_non_empty[field] = [json.dumps(val) for val in value]

        multiplied = [dict(zip(obj_non_empty.keys(), item))
                      for item in list(itertools.product(*[values for key, values in obj_non_empty.items()]))]

        # break nested fields back out
        for frag in [{**obj_empty, **item} for item in multiplied]:
            processed_frag = {}

            for key, value in frag.items():
                # ":" in unique id will make json.loads error
                try:
                    val = json.loads(value)
                except json.decoder.JSONDecodeError:
                    val = value

                processed_frag.update({key: val})

                # If it's a dict, we need to make sure we only have the subfields needed
                if isinstance(val, dict):
                    for subfield in nest_map[key]:
                        value = self.get_recursive(val, subfield.split('.'))
                        processed_frag.update({f"{key}.{subfield}": value})

                    # Now that we've pulled out the subfields, if the root field itself isn't
                    # used in ER (which it shouldn't be, but who knows) then it's safe to pop
                    if key not in field_names:
                        processed_frag.pop(key)

            yield processed_frag

    def get_fragments(self, strategy_name) -> Generator[dict, None, None]:
        """
        Primary method of execution for producing fragments from entlets.

        Isolates the "root" keys that will be used in a given strategy (comes from config), then
        multiplies the values out using the ._product_fragment_product() method.

        Ensures all fragments retain the "entlet_id" field, which is critical for ER.

        Args:
            strategy_name (str): The name of the strategy.

        Returns:

        """

        # Entlet is empty
        if not self.values:
            return

        # Grab all root keys that will be used in the fragment
        base_fields = list(set([field.split('.')[0] for field in self.er_fields[strategy_name]]))
        frag_root_fields = {key: self.values[key] for key in base_fields}

        for fragment in self._produce_fragment_product(frag_root_fields, self.er_fields[strategy_name]):
            # Make sure entlet_id makes it into every fragment
            fragment["entlet_id"] = self["entlet_id"]
            yield fragment

    def is_subset(self, other_entlet):
        """ Useful for unit tests, check if an entlet contains all the values of another """
        for group_name, attr_groups in self.values.items():
            # If a group type is missing, then not a subset.  Currently requiring groups to line up even though they
            # have no bearing on the final frags/entlet
            if group_name not in other_entlet.values:
                return False

            # Compare the values w/i the group
            other_attr_groups = other_entlet.values[group_name]
            for attr_group in attr_groups:
                for other_attr_group in other_attr_groups:
                    if all(other_attr_group.get(k) == v for k, v in attr_group.items()):
                        break
                # Couldn't find an attribute match for one of the groups
                else:
                    return False

        # Now check const values
        for field, value in self.const_values.items():
            if field not in other_entlet.const_values:
                return False

            if value != other_entlet.const_values[field]:
                return False

        return True

