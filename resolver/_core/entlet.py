
from __future__ import annotations
from collections import defaultdict
from copy import deepcopy
from hashlib import md5
import itertools
import json
from typing import Any, Callable, Dict, Generator, List, Tuple, Union

from resolver._utils.functions import merge_union

# TODO: Values should have their type stored in the class for checking to avoid collisions
# TODO: Deduplication of values could be moved to post-munge for efficiency


class Entlet(object):
    """
    This class lets you build up information about an entlet and then output it into flattened fragments.

    NOTE: the following fields are reserved
      - ent_type
      - entlet_id

    """
    UID_FIELDS = ['data_source', 'ent_type']

    CUSTOM_UID_FIELDS = None
    SOURCE_UID_FIELD = None

    def __init__(self):
        self.values = defaultdict(list)

        self.fields = set()
        self.const_values = {}

        self._uid = None

    @property
    def required_fields(self) -> List[str]:
        return [*self.UID_FIELDS, 'entlet_id']

    @property
    def entlet_id(self):
        """Retrieves the entlet id. If this is the first time calling this property, will generate
        the id and save it to the instance's const_values"""
        return self.const_values.get("entlet_id", self._generate_entlet_id())

    def __getitem__(self, key: str):
        """
        Retrieves a value stored on the entlet. Will search constants first (reserved for
        required fields).

        Args:
            key (str): The key to retrieve the value for

        Returns:
            (Any) The value of the last provided key
        """
        if key in self.const_values:
            return self.const_values[key]

        return self.get_recursive(self.values, key.split('.'))

    def __repr__(self):
        """ So we can easily see in the debugger """
        attrs = ', '.join('{}={}'.format(k, v) for k, v in self.const_values.items())
        return f'Entlet({attrs})'

    def __contains__(self, o):
        """Boolean check if key exists in .values"""
        if self.const_values.__contains__(o):
            return True

        try:
            self.get_recursive(self.values, o.split('.'))
            return True
        except KeyError:
            return False

    def get(self, key: str, default: Any = None) -> Any:
        """
        Provides the same 'get' functionality as exists in a dictionary.

        Args:
            key (str):
            default (Any):

        Returns:

        """
        if key in self:
            return self[key]

        return default

    @classmethod
    def define_custom_uid_fields(cls, *args):
        """
        Defines a set of fields that will be used to create a custom id. Should be used only if
        the source of the entlet does not provide its own unique id.

        Before you use this, remember that the entlet ID includes the entity type and the source.
        If your data source provides some sort of not-very-unique ID like "123", then it should
        still be good enough to uniquely identify the entlet so long as the ID is unique within the
        data source itself.

        If you still decide to create custom uids with this method, the custom id will be generated
        according to the following process:
            1. stringify all values stored in the specified fields
            2. sort and deduplicate the values
            3. join and hash the values

        This will keep the entlet IDs stable between runs if the underlying information doesn't change.

        inb4 "why hashing?" -DavaiSyka1@3 entlet ids enforce a consistent structure of {source}:{ent_type}:{uid}. Imagine
        searching for (or having a url route for) a bunch of stringified json. Nightmares.

        Args:
            *args (str): The fields to use

        Returns:
            (cls)
        """
        cls.CUSTOM_UID_FIELDS = sorted(args)
        return cls

    @classmethod
    def define_source_uid_field(cls, field: str):
        """
        Defines which field from a given source constitutes a unique id. This is the recommended way of
        creating the entlet unique ID.

        Keep in mind that declaring a field in this way forces the field to become a single-value constant.
        For example, if you declare the field "state_iso2" as the source uid field, you may only set the value
        of field "state_iso2" once.

        Example:
        Say you have a record from the data source "US_STATES" and you're creating a "state" entity. The
        record looks like:
        {
            "state_iso2": "NY",
            "population": "a lot"
        }
        ...If you pass "state_iso2" to this method, then the resulting entlet id would look like:
            "US_STATES:state:NY"

        Args:
            field: The name of the field that represents a source-provided unique ID.

        Returns:
            (cls)
        """
        if cls.CUSTOM_UID_FIELDS:
            raise ValueError("You cannot declare both custom fields and a source id field")

        cls.SOURCE_UID_FIELD = field
        return cls

    def define_individual_uid(self, uid: str):
        """
        Hard set a particular value to a particular entlet's entlet_id.

        If you use this method it's recommended you take steps to ensure uniqueness across datasets.
        Keep in mind that entlets with the same ID will be merged NO MATTER WHAT. So if you create
        entlets from Source A with IDs auto-incremented from 1 and do the same for source B, you're
        probably going to have a lot of strange resolutions (I'm not going to stop you from wasting
        your own time).

        Args:
            uid (str): The unique ID you are passing

        Returns:

        """
        if 'entlet_id' in self.const_values:
            raise ValueError("An entlet id has already been declared.")

        self._uid = uid
        return self

    def _generate_entlet_id(self):
        """
        Will create a "unique" entlet id based on supplied settings and store the resulting ID in self.values
        under "entlet_id"

        Returns:
            (self)
        """
        if self._uid:
            return self._generate_id_from_individual()

        # Preferred method, so considered first
        if self.SOURCE_UID_FIELD:
            return self._generate_id_from_source_field()

        # Least preferred method, so considered last
        if self.CUSTOM_UID_FIELDS:
            return self._generate_id_from_custom_fields()

        raise ValueError("Unable to generate entlet id. No id generation method has been "
                         "specified for the entlet, nor has a custom id been provided.")

    def _generate_id_from_individual(self):
        """
        Generates an entlet id from a unique id provided to the entlet instance
        """
        uid_values = [self.values.get(field, None) for field in self.UID_FIELDS]
        if not all(uid_values):
            missing = [field for field in self.UID_FIELDS if not self[field]]
            raise ValueError(f"Entlet is missing the following required fields: {missing}")

        uid = ':'.join([*uid_values, self._uid])
        self.const_values["entlet_id"] = uid
        return uid

    def _generate_id_from_source_field(self):
        """
        Generates a unique id for the entlet assuming a source uid field has been declared.

        Returns:
            str
        """
        uid_values = [self.values.get(field, None) for field in (*self.UID_FIELDS, self.SOURCE_UID_FIELD)]
        if not all(uid_values):
            missing = [field for field in self.UID_FIELDS if not self[field]]
            raise ValueError(f"Entlet is missing the following required fields: {missing}")

        uid = ':'.join(uid_values)
        self.const_values["entlet_id"] = uid
        return uid

    def _generate_id_from_custom_fields(self):
        """
        Generates a unique-ish id from a list of fields. Values from the fields get
        stringified, sorted, and hashed to produce the id.

        The end result will look something like:
        "{source}:{ent_type}:4297f44b13955235245b2497399d7a93"

        Returns:
            (str) the generated id
        """
        uid_values = [val for field in self.UID_FIELDS for val in self.get(field, [])]
        custom_values = [self.values.get(value, None) for value in self.CUSTOM_UID_FIELDS]

        hashed_custom_values = md5(
            ''.join(sorted([str(val) for val in custom_values])).encode('utf-8')
        ).hexdigest()

        uid = ":".join([*uid_values, hashed_custom_values])
        self.const_values["entlet_id"] = uid
        return uid

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

        # Sub-structure is a list, iterate through
        if isinstance(obj, list):
            return [self.get_recursive(sub_obj, key_parts) for sub_obj in obj]

        # Look for the next part of the path
        top_level = key_parts.pop(0)
        if top_level not in obj:
            raise KeyError(f"Key not found: {top_level}")

        return self.get_recursive(obj[top_level], key_parts)

    def add(self, obj: Dict[str, Any]) -> Entlet:
        """
        Add values to the Entlet instance. All entlet values (except special reserved fields - see below)
        are stored in lists and their data structures are preserved - see below examples. Values passed
        as a list will be unpacked.

        Reserved fields - fields that are used to generate the entlet_id via the define_source_uid_field
        and define_individual_uid methods - are NOT stored as lists because values must be singular in order
        to generate the id.

        Args:
            obj (Dict[str, Any])

        Returns:
            self

        Examples:
            # Add single value to a field. The key will be used as the field name.
            entlet = Entlet()
            entlet.add({"a": 1, "b": 2})

            # Result: {"a": [1], "b": [2]}


            # Add nested fields in order to retain associated between the values.
            entlet = Entlet()
            entlet.add({a: {"b": 1, "c": 2}})

            # Result: {"name": [{"b": 1, "c": 2}]}


            # Add multiple values in one call.
            entlet = Entlet()
            entlet.add({a: [1, 2], b: [{"b": 3, "c": 4}, {"b": 5, "c": 6}]})

            # Result: {"a": [1, 2], "b": [{"b": 3, "c": 4}, {"b": 5, "c": 6}]}


            # Duplicates are dropped within each key. Nested data structures only deduplicate if the
            # entire structure is duplicated.
            entlet = Entlet()
            entlet.add({a: 1, b: 2, c: {"c": 2, "d": 3}})
            entlet.add({a: 1, b: 3, c: {"c": 2, "d": 4}})
            entlet.add({c: {"c": 2, "d": 3}})

            # Result: {"a": [1], "b": [2, 3], "c": [{"c": 2, "d": 3}, {"c": 2, "d": 4}]}


            # Values in a field must be of the same datatype
            entlet = Entlet()
            entlet.add({a: [1, 2, "example"]})

            # Result: ValueError


            # The key entlet_id is reserved and cannot be added
            entlet = Entlet()
            entlet.add({entlet_id: "id123", ent_type: "entity"})

            # Result: KeyError

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
            if not key:
                raise KeyError("Cannot add entlet property which evaluates to None.")

            if key == "entlet_id":
                raise KeyError("You cannot use the .add() method to declare an entlet id.")

            if key in self.required_fields or key == self.SOURCE_UID_FIELD:
                self._set_const({key: obj[key]})
                self.values[key] = obj[key]
                continue

            if key not in self.values or not self.values[key]:
                if isinstance(obj[key], list):
                    self.values[key] += obj[key]

                else:
                    self.values[key].append(obj[key])
            else:
                merge_values(self.values[key], obj[key], key)

        return self

    def merge(self, entlet: Entlet) -> Entlet:
        """
        Merges another entlet instance into this one.

        Accepts the .dump() from another entlet. Should not be used outside of rollup.
        Functionally the same as .add() except doesn't check for constant fields

        Args:
            entlet: keys and values

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

        for key in entlet.dump():
            if key not in self.values:
                if isinstance(entlet[key], list):
                    self.values[key] += entlet[key]

                elif isinstance(self.values[key], list):
                    self.values[key].append(entlet[key])

                else:
                    self.values[key] = [self.values[key], entlet[key]]

            else:
                merge_values(self.values[key], entlet[key], key)

        return self

    def _set_const(self, obj):
        """
        Workaround for python's lack of ability to set constant values. Ensures the entlet's constant values
        don't get overwritten, which would indicate a problem with munge.

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
                raise ValueError(
                    "Field {field} is protected and requires values of type string - "
                    "got {type(value)}."
                )

            self.const_values[field] = value

    def dump(self) -> Dict[str, Union[list, str]]:
        """
        Return all information stored on the entlet as a dictionary. Structures are deep copied
        to avoid unexpected mutation.

        Returns:
            Dict[str, Union[list, str]]
        """
        values = deepcopy(dict(self.values))
        values.update(self.const_values.copy())
        return values

    def standardize_values(
            self,
            standardization_field: str,
            filter_rulesets: List[Dict[str, Union[str, Callable]]],
            standardization_function: Callable
    ) -> Entlet:
        """
        Substitutes values for their standardized counterparts. Values that are substituted get moved
        to a new '{field_name}_raw' field.

        **IMPORTANT**
        Standardization can be either scoped or "global" based on applied filters

        Filters must follow the below data structure:
        {
            "field_name": the name of the field
            "comparator": a callable that accepts a two arguments,
            "values": ...one or more values...
        }

        Args:
            standardization_field (str): The name of the field to be standardized
            filter_rulesets (List[Dict]: See above.
            standardization_function (Callable): The function by which to compare the value on the
                                                 entlet versus the value supplied by the filter_value
                                                 parameter.

        Returns:
            (self) The same entlet instance, with the standardization rule applied.
        """
        if standardization_field not in self:
            return self

        root_field = standardization_field.split('.', 1)[0]

        # Global filters are applied against root fields other than the value being standardized
        # e.g., standardizing 'state' but applying a filter against 'country'
        global_filters = [fltr for fltr in filter_rulesets if fltr['field_name'].split('.', 1)[0] != root_field]
        for global_filter in global_filters:
            # All global filters must pass in order for standardization to occur
            if not self._test_global_filter(**global_filter):
                return self

        # Scoped filters are applied against fields in the same scope as the values being standardized
        # e.g., standardizing 'state.iso2' but applying a filter against 'state.type'
        scoped_filters = [fltr for fltr in filter_rulesets if fltr['field_name'].split('.', 1)[0] == root_field]
        values = self[root_field]
        for scoped_filter in scoped_filters:
            # Whittle down the list of values by applying filters in sequence
            values = self._apply_scoped_filter(
                values,
                scoped_filter["field_name"],
                scoped_filter["value"],
                scoped_filter["comparator"]
            )

        # TODO: Refactor. Seems to work for now but i don't know how
        for value in values.copy():
            if isinstance(value, dict):
                original_value = self.get_recursive(value, standardization_field.split('.')[1:])
            else:
                original_value = value

            standardized_value = standardization_function(original_value)

            # If the field is nested, we add a {field}_raw field on to the nested object
            if isinstance(value, dict):
                parent_of_value = self.get_recursive(
                    value,
                    standardization_field.split('.')[1:-1]
                )
                parent_of_value.update({
                    f"{standardization_field.split('.')[-1]}_raw": original_value,
                    standardization_field.split('.')[-1]: standardized_value
                })
                continue

            # If the field isn't nested, the old values get pushed to a new root field
            self[standardization_field].remove(original_value)
            self.add({
                standardization_field: standardized_value,
                f"{standardization_field}_raw": original_value
            })

        return self

    def _apply_scoped_filter(
            self,
            values: List[Any],
            filter_field: str,
            filter_value: Any,
            filter_comparator: Callable
    ):
        """
        Filters a list of data structures down to just the ones that pass the provided filter logic.

        Root key for the standardization field and filter are the same - filters are locally
        scoped such that individual nested objects must pass the filter

        Args:
            values (List[Any]): The list of data structures to be filtered
            filter_field (str): The name of the field on the data structure whos value is being evaluated
            filter_value (Any): The value that will be compared against the value provided by filter_field
            filter_comparator (Callable): The function that will evaluate filter_value against the
                                          the value provided by filter_field. Must return a boolean.

        Returns:
            List[Any] The same list passed to the 'values' param, reduced to only those which passed
            the filter
        """
        split = filter_field.split('.')
        if len(split) > 1:
            filter_nested_field = split[1:]
        else:
            filter_nested_field = split

        passed_filter = []

        for value in values:
            check_value = self.get_recursive(value, filter_nested_field)
            if filter_comparator(check_value, filter_value):
                passed_filter.append(value)

        return passed_filter

    def _test_global_filter(
            self,
            field_name: str,
            value: Any,
            comparator: Callable
    ) -> bool:
        """
        Test whether this entlet instance can pass a globally scoped filter.

        **IMPORTANT**
        When applying a global filter, only ONE value has to pass for the filter to pass.
        For example, if a filter is "country equals 'US'" and this entlet contains two
        values under country - "US" and "UK" - then the entlet passes.

        Args:
            field_name (str): The name of the field whose values determine the filter outcome
            value (Any): The value to compare against the field's values
            comparator (Callable): The method by which the values of 'filter_value' and
                                   the entlet's values will be compared.

        Returns:
            (bool) True if this entlet passes the filter, or False if not
        """

        filter_field_root_key = field_name.split('.')[0]
        filter_nested_field = '.'.join(field_name.split('.')[1:])

        if any(comparator(
                self.get_recursive(filter_obj, filter_nested_field.split('.')),
                value
            ) for filter_obj in self[filter_field_root_key]
        ):
            return True

        return False

    def clear(self) -> Entlet:
        """Removes all values stored on the entlet. Really should only be used in testing."""
        self.values.clear()
        self.const_values.clear()
        return self

    def _produce_fragment_product(self, obj: Dict[str, Any], field_names: List[str]):
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
                except (json.decoder.JSONDecodeError, TypeError):
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

    def get_fragments(self, fragment_fields: List[str]) -> Generator[dict, None, None]:
        """
        Primary method of execution for producing fragments from entlets.

        Isolates the "root" keys that will be used in a given strategy (comes from config), then
        multiplies the values out using the ._product_fragment_product() method.

        Ensures all fragments retain the "entlet_id" field, which is critical for ER.

        Args:
            fragment_fields (List[str]): The list of fields that will be used in fragmenting

        Returns:

        """

        # Entlet is empty
        if not self.values:
            return

        # Grab all root keys that will be used in the fragment
        base_fields = list(set([field.split('.')[0] for field in fragment_fields]))
        frag_root_fields = {key: self.values[key] for key in base_fields}

        for fragment in self._produce_fragment_product(frag_root_fields, fragment_fields):
            # The entlet_id *must* be appended to every fragment
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

