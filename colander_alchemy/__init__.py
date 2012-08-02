import inspect

import colander
import pytz
from sqlalchemy import types
from sqlalchemy.orm.properties import RelationshipProperty, ColumnProperty


missing = colander.null


class StrippedString(colander.String):
    def deserialize(self, node, cstruct):
        value = super(StrippedString, self).deserialize(node, cstruct)

        if value is colander.null:
            return value

        return value.strip()


def remove_nulls(data):
    """
    Remove all colander.null values from given data dict

    This function is smart enough to understand nested dicts

    Examples::

        >>> remove_nulls({'key1': colander.null, 'key2': 1})
        {'key2': 1}
        >>> remove_nulls({'a': {'b': colander.null}})
        {'a': {}}
    """
    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            result[key] = remove_nulls(value)
        elif value is colander.null:
            pass
        else:
            result[key] = value
    return result


def nullable(node):
    """
    This function should be used when declaring related nullable schemas.

    For example::

        class PhonenumberSchema(SchemaNode):
            number = SchemaNode(Integer())


        class Person(SchemaNode):
            phonenumber = nullable(PhonenumberSchema())
    """
    _deserialize = node.deserialize

    def wrapper(cstruct=colander.null):
        if cstruct is None or cstruct == '':
            return None
        return _deserialize(cstruct)

    node.deserialize = wrapper
    return node


class NullableSchemaNode(colander.SchemaNode):
    def deserialize(self, cstruct=colander.null):
        if cstruct is None or cstruct == '':
            return None
        return super(NullableSchemaNode, self).deserialize(cstruct)


class NaiveDateTime(colander.DateTime):
    """Converts deserialized datetimes to UTC and removes tzinfo."""
    def deserialize(self, node, cstruct):
        result = super(NaiveDateTime, self).deserialize(node, cstruct)
        if result is not colander.null:
            result = result.astimezone(pytz.utc).replace(tzinfo=None)
        return result


class ColanderAlchemyMixin(object):
    __schema__ = {}

    @classmethod
    def _schema_validate(cls, node, value):
        pass

    @classmethod
    def schema(cls,
               include=None,
               exclude=None,
               name='',
               missing=colander.required,
               assign_defaults=True):
        generator = cls.__schema_generator__(cls, missing, assign_defaults)
        return generator.create(include, exclude, name)

    @classmethod
    def get_create_schema(
            cls,
            include=None,
            exclude=None,
            missing=colander.required,
            assign_defaults=True):
        generator = cls.__schema_generator__(cls, missing, assign_defaults,
                                             cls._schema_validate)
        return generator.create(include, exclude)

    @classmethod
    def get_update_schema(
            cls,
            include=None,
            exclude=None,
            missing=missing,
            assign_defaults=False):
        generator = cls.__schema_generator__(cls, missing, assign_defaults,
                                             cls._schema_validate)
        return generator.create(include, exclude)

    @classmethod
    def get_search_schema(
            cls,
            include=None,
            exclude=None,
            missing=missing,
            assign_defaults=False):
        generator = cls.__schema_generator__(
            cls,
            missing,
            assign_defaults,
            cls._schema_validate,
            only_indexed_fields=True,
            include_primary_keys=True,
            include_relations=False
        )
        return generator.create(include, exclude)


class UnknownTypeException(Exception):
    def __init__(self, type):
        self.type = type

    def __str__(self):
        return 'Unknown type %r' % self.type


class SchemaGenerator(object):
    TYPE_MAP = {
        types.BigInteger: colander.Integer,
        types.SmallInteger: colander.Integer,
        types.Integer: colander.Integer,
        types.DateTime: colander.DateTime,
        types.Date: colander.Date,
        types.Time: colander.Time,
        types.Text: colander.String,
        types.Unicode: colander.String,
        types.UnicodeText: colander.String,
        types.Float: colander.Float,
        types.Numeric: colander.Decimal,
        types.Boolean: colander.Boolean
    }

    def __init__(self, model_class, missing=colander.required,
                 assign_defaults=True, validator=None,
                 only_indexed_fields=False, include_primary_keys=False,
                 include_relations=True):
        self.validator = validator
        self.model_class = model_class
        self.missing = missing
        self.assign_defaults = assign_defaults
        self.only_indexed_fields = only_indexed_fields
        self.include_primary_keys = include_primary_keys
        self.include_relations = include_relations

    def create(self, include=None, exclude=None, name=''):
        colander_schema = colander.SchemaNode(
            colander.Mapping(),
            name=name,
            missing=self.missing,
            validator=self.validator
        )

        fields = set(self.model_class._sa_class_manager.values())
        tmp = []
        for field in fields:
            column = field.property
            if isinstance(column, ColumnProperty) and self.skip_column(column):
                continue
            tmp.append(field)
        fields = set(tmp)

        if include:
            fields.update(set(
                [getattr(self.model_class, field) for field in include]
            ))

        if exclude:
            func = lambda a: a.key not in exclude
            fields = filter(func, fields)

        return self.get_create_schema_nodes(colander_schema, fields)

    def get_create_schema_nodes(self, schema, fields):
        for field in fields:
            column = field.property

            schema_node = None
            if isinstance(column, RelationshipProperty):
                if self.include_relations:
                    schema_node = self.relation_schema_node(column)
            elif isinstance(column, ColumnProperty):
                schema_node = self.column_schema_node(column)
            if not schema_node:
                continue
            schema.add(schema_node)
        return schema

    def relation_schema_node(self, relation_property):
        model = relation_property.argument
        if model.__class__.__name__ == 'function':
            # for string based relations (relations where the first
            # argument is a classname string instead of actual class)
            # sqlalchemy generates return_cls functions which we need
            # to call in order to obtain the actual model class
            model = model()

        name = relation_property.key

        if name not in self.model_class.__schema__:
            return None

        if not inspect.isclass(model) or \
                not issubclass(model, ColanderAlchemyMixin):
            raise Exception('Could not create schema for %r' % model)
        else:
            if self.is_nullable(name):
                default = missing
            else:
                default = self.missing
            kwargs = {
                'name': name,
                'missing': default,
                'assign_defaults': self.assign_defaults
            }
            try:
                schema_creator = self.model_class.__schema__[name]['schema']
            except KeyError:
                schema_creator = model.schema
                del kwargs['assign_defaults']

            schema_node = schema_creator(**kwargs)
            if self.is_nullable(name):
                schema_node = nullable(schema_node)
            return schema_node

    def is_nullable(self, name):
        try:
            return self.model_class.__schema__[name]['nullable']
        except KeyError:
            return True

    def is_read_only(self, name):
        try:
            return self.model_class.__schema__[name]['readonly']
        except KeyError:
            return False

    def validators(self, name):
        try:
            return self.model_class.__schema__[name]['validator']
        except KeyError:
            return None

    def skip_column(self, column_property):
        column = column_property.columns[0]
        if (not self.include_primary_keys and column.primary_key or
                column.foreign_keys):
            return True

        if (self.is_read_only(column.name) or
                column_property._is_polymorphic_discriminator):
            return True

        if self.only_indexed_fields and not self.has_index(column):
            return True
        return False

    def has_index(self, column):
        if column.primary_key or column.foreign_keys:
            return True
        table = column.table
        for index in table.indexes:
            if len(index.columns) == 1 and column.name in index.columns:
                return True
        return False

    def column_schema_node(self, column_property):
        column = column_property.columns[0]
        name = column.name

        colander_type = self.convert_type(column.type)
        if column.default and self.assign_defaults:
            default = column.default.arg
        else:
            if self.is_nullable(name) and column.nullable:
                default = missing
            else:
                default = self.missing

        validator = self.length_validator(column)
        if column.nullable:
            schema_node_cls = NullableSchemaNode
        else:
            schema_node_cls = colander.SchemaNode

        return schema_node_cls(
            colander_type,
            name=name,
            missing=default,
            validator=validator
        )

    def length_validator(self, column):
        """
        Returns colander length validator for given column
        """
        validator = self.validators(column.name)
        if hasattr(column.type, 'length'):
            length = colander.Length(max=column.type.length)
            if validator:
                if isinstance(validator, colander.All):
                    return colander.All(length, *validator.validators)
                else:
                    return colander.All(length, validator)
            else:
                return length
        return validator

    def convert_type(self, column_type):
        for class_ in self.TYPE_MAP:
            # Float type in sqlalchemy inherits numeric type, hence we need
            # the following check
            if column_type.__class__ is types.Float:
                if isinstance(column_type, types.Float):
                    return self.TYPE_MAP[types.Float]()
            if isinstance(column_type, class_):
                return self.TYPE_MAP[class_]()
        raise UnknownTypeException(column_type)


ColanderAlchemyMixin.__schema_generator__ = SchemaGenerator
