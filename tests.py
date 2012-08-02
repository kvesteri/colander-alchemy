from datetime import datetime

import colander
from pytest import raises
from colander import Range, required, Length, OneOf, All, Email, null
from colander.tests.test_colander import DummySchemaNode
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.types import BigInteger
from sqlalchemy.ext.declarative import declarative_base

from colander_alchemy import (
    ColanderAlchemyMixin,
    NaiveDateTime,
    NullableSchemaNode,
    SchemaGenerator,
    UnknownTypeException,
    missing,
    remove_nulls
)


Base = declarative_base()


class RelatedClassAA(Base, ColanderAlchemyMixin):
    __tablename__ = 'another_related_class'
    id = sa.Column(BigInteger, autoincrement=True, primary_key=True)
    integer_field = sa.Column(sa.Integer)
    text_field = sa.Column(sa.Text)


class RelatedClassA(Base, ColanderAlchemyMixin):
    __tablename__ = 'related_class_a'
    id = sa.Column(BigInteger, autoincrement=True, primary_key=True)
    integer_field = sa.Column(sa.Integer)
    text_field = sa.Column(sa.Text)
    foreign_key_field = sa.Column(None, sa.ForeignKey(RelatedClassAA.id))

    related = orm.relationship(RelatedClassAA)


class RelatedClassB(Base, ColanderAlchemyMixin):
    __tablename__ = 'related_class_b'
    id = sa.Column(BigInteger, autoincrement=True, primary_key=True)
    text_field = sa.Column(sa.Text)


class RelatedClassC(Base, ColanderAlchemyMixin):
    __tablename__ = 'related_class_c'
    id = sa.Column(BigInteger, autoincrement=True, primary_key=True)
    text_field = sa.Column(sa.Text)


class ColanderSchemaTestModel(Base, ColanderAlchemyMixin):
    __tablename__ = 'colander_schema_test'

    id = sa.Column(BigInteger, autoincrement=True, primary_key=True)
    foreign_key_field = sa.Column(None, sa.ForeignKey(RelatedClassA.id))
    foreign_key_field2 = sa.Column(None, sa.ForeignKey(RelatedClassB.id))
    foreign_key_field3 = sa.Column(None, sa.ForeignKey(RelatedClassC.id))
    big_integer_field = sa.Column(BigInteger)
    integer_field = sa.Column(sa.Integer, nullable=False)
    numeric_field = sa.Column(sa.Numeric)
    float_field = sa.Column(sa.Float)
    datetime_field = sa.Column(sa.DateTime, index=True)
    date_field = sa.Column(sa.Date)
    time_field = sa.Column(sa.Time)
    text_field = sa.Column(sa.Text)
    unicode_field = sa.Column(sa.Unicode(255))
    unicode_field2 = sa.Column(sa.Unicode(20))
    unicode_field3 = sa.Column(sa.Unicode(20))
    unicode_field4 = sa.Column(sa.Unicode(20))
    unicode_text_field = sa.Column(sa.UnicodeText)
    field_with_range = sa.Column(sa.Integer)
    nullable_field = sa.Column(sa.Boolean, nullable=True)
    not_nullable_field = sa.Column(sa.Boolean, nullable=False, default=False)
    read_only_field = sa.Column(sa.Integer)

    whitelisted_relation = orm.relationship(RelatedClassA)
    read_only_relation = orm.relationship(RelatedClassB)
    not_nullable_relation = orm.relationship(RelatedClassC)

    __schema__ = {
        'read_only_field': {'readonly': True},
        'field_with_range': {'validator': Range(min=1, max=99)},
        'whitelisted_relation': {},
        'not_nullable_relation': {'nullable': False},
        'unicode_field3': {'validator': OneOf(['choice'])},
        'unicode_field4': {'validator': All(OneOf(['choice']), Email())}
    }


class ColanderMixinTestCase(object):
    def find_field(self,
                   field,
                   schema=None,
                   missing=required,
                   include=None,
                   assign_defaults=True):
        if not schema:
            schema = ColanderSchemaTestModel.schema(
                missing=missing,
                assign_defaults=assign_defaults,
                include=include
            )
        for node in schema.children:
            if node.name == field:
                return node
        return None


class TestRemoveNulls(object):
    def test_removes_all_keys_with_nulls(self):
        assert remove_nulls({'a': null}) == {}


class TestNaiveDateTime(object):
    def test_deserialize_naive_datetime(self):
        type_ = NaiveDateTime()
        node = DummySchemaNode(None)
        result = type_.deserialize(node, '2011-07-28T17:18:00')
        assert result == datetime(2011, 7, 28, 17, 18)
        assert result.tzinfo is None

    def test_deserialize_utc_datetime(self):
        type_ = NaiveDateTime()
        node = DummySchemaNode(None)
        result = type_.deserialize(node, '2011-07-28T17:18:00Z')
        assert result == datetime(2011, 7, 28, 17, 18)
        assert result.tzinfo is None

    def test_deserialize_offset_aware_datetime(self):
        type_ = NaiveDateTime()
        node = DummySchemaNode(None)
        result = type_.deserialize(node, '2011-07-28T17:18:00+02:00')
        assert result == datetime(2011, 7, 28, 15, 18)
        assert result.tzinfo is None


class TestSearchSchemaGeneration(ColanderMixinTestCase):
    def test_includes_only_indexed_fields(self):
        schema = ColanderSchemaTestModel.get_search_schema()

        assert self.find_field('id', schema) is not None
        assert self.find_field('datetime_field', schema) is not None
        assert self.find_field('time_field', schema) is None
        assert self.find_field('whitelisted_relation', schema) is None


class TestColanderSchemaMixin(ColanderMixinTestCase):
    def test_skips_primary_keys_by_default(self):
        assert not self.find_field('id')

    def test_skips_foreign_keys_by_default(self):
        assert not self.find_field('foreign_key_field')

    def test_can_include_foreign_keys(self):
        assert self.find_field(
            'foreign_key_field',
            include=['foreign_key_field']
        )

    def test_skips_readonly_fields(self):
        assert not self.find_field('read_only_field')

    def test_supports_validators(self):
        field = self.find_field('field_with_range')
        assert isinstance(field.validator, Range)

    def test_supports_nullable_fields(self):
        field = self.find_field('nullable_field')
        assert isinstance(field, NullableSchemaNode)

    def test_supports_related_attributes(self):
        schema = self.find_field('related')
        field = self.find_field('text_field', schema)
        assert isinstance(field.typ, colander.String)


class TestRelations(ColanderMixinTestCase):
    def test_skips_readonly_relations(self):
        assert not self.find_field('read_only_relation')

    def test_supports_related_attributes(self):
        schema = self.find_field('related')
        field = self.find_field('text_field', schema)
        assert isinstance(field.typ, colander.String)

    def test_supports_relation_naming(self):
        field = self.find_field('whitelisted_relation')

        assert field.name == 'whitelisted_relation'

    def test_relations_nullable_by_default(self):
        field = self.find_field('whitelisted_relation', missing=missing)

        assert field.deserialize(None) is None

    def test_supports_not_nullable_relations(self):
        field = self.find_field('not_nullable_relation', missing=missing)

        with raises(Exception):
            field.deserialize(None)


class TestDefaultValues(ColanderMixinTestCase):
    def test_supports_default_values(self):
        field = self.find_field('not_nullable_field')
        assert field.missing is False

    def test_nullable_field_default_is_missing_object(self):
        field = self.find_field('nullable_field')
        assert field.missing == missing

    def test_not_nullable_field_default_is_required_object(self):
        field = self.find_field('integer_field')
        assert field.missing == required

    def test_missing_value_default_can_be_overridden(self):
        schema = ColanderSchemaTestModel.schema(missing=missing)
        for node in schema.children:
            if node.name == 'nullable_field':
                break
        assert node.missing == missing


class TestAutomaticLengthValidations(ColanderMixinTestCase):
    def test_auto_adds_length_validator(self):
        field = self.find_field('unicode_field2')
        assert isinstance(field.validator, Length)

    def test_creates_composite_validator_if_validator_already_exists(self):
        field = self.find_field('unicode_field3')
        assert isinstance(field.validator, All)

    def test_appends_validator_if_composite_validator_already_exists(self):
        field = self.find_field('unicode_field4')
        assert isinstance(field.validator, All)
        assert len(field.validator.validators) == 3


class TestTypeConversion(ColanderMixinTestCase):
    def test_big_integer_converts_to_colander_integer(self):
        field = self.find_field('big_integer_field')
        assert isinstance(field.typ, colander.Integer)

    def test_integer_converts_to_colander_integer(self):
        field = self.find_field('integer_field')
        assert isinstance(field.typ, colander.Integer)

    def test_numeric_converts_to_colander_decimal(self):
        field = self.find_field('numeric_field')
        assert isinstance(field.typ, colander.Decimal)

    def test_float_converts_to_colander_float(self):
        field = self.find_field('float_field')
        assert isinstance(field.typ, colander.Float)

    def test_datetime_converts_to_colander_datetime(self):
        field = self.find_field('datetime_field')
        assert isinstance(field.typ, colander.DateTime)

    def test_date_converts_to_colander_string(self):
        field = self.find_field('date_field')
        assert isinstance(field.typ, colander.Date)

    def test_time_converts_to_colander_time(self):
        field = self.find_field('time_field')
        assert isinstance(field.typ, colander.Time)

    def test_unicode_converts_to_colander_string(self):
        field = self.find_field('unicode_field')
        assert isinstance(field.typ, colander.String)

    def test_unicode_text_converts_to_colander_string(self):
        field = self.find_field('unicode_text_field')
        assert isinstance(field.typ, colander.String)

    def test_text_converts_to_colander_string(self):
        field = self.find_field('text_field')
        assert isinstance(field.typ, colander.String)

    def test_supports_type_overriding(self):
        SchemaGenerator.TYPE_MAP[sa.DateTime] = NaiveDateTime

        field = self.find_field('datetime_field')
        assert isinstance(field.typ, NaiveDateTime)

    def test_throws_exception_for_unknown_type(self):
        class UnknownType:
            pass
        with raises(UnknownTypeException):
            SchemaGenerator(ColanderSchemaTestModel) \
                .convert_type(UnknownType())
