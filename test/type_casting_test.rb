# frozen_string_literal: true

require 'test_helper'

class TypeCastingTest < Minitest::Test
  def test_casting_to_boolean
    ['1', 'true', 1, true].each do |value|
      assert NdrAvro::TypeCasting.cast_to_avro_datatype(value, :boolean)
    end

    ['0', 'false', 0, false].each do |value|
      refute NdrAvro::TypeCasting.cast_to_avro_datatype(value, :boolean)
    end

    assert_nil NdrAvro::TypeCasting.cast_to_avro_datatype(nil, :boolean)
  end

  def test_casting_to_int
    assert_equal 12, NdrAvro::TypeCasting.cast_to_avro_datatype('12', :int)
    assert_equal 13, NdrAvro::TypeCasting.cast_to_avro_datatype(13, :int)

    assert_nil NdrAvro::TypeCasting.cast_to_avro_datatype(nil, :int)
  end

  def test_casting_to_long
    assert_equal 9223372036854775807,
                 NdrAvro::TypeCasting.cast_to_avro_datatype('9223372036854775807', :long)
    assert_equal 13, NdrAvro::TypeCasting.cast_to_avro_datatype(13, :long)

    assert_nil NdrAvro::TypeCasting.cast_to_avro_datatype(nil, :long)
  end

  def test_casting_to_float
    assert_in_delta(1.2345678901234567e+19, NdrAvro::TypeCasting.cast_to_avro_datatype('12345678901234567890.12', :float))
    assert_in_delta(1.2345678901234567e+19, NdrAvro::TypeCasting.cast_to_avro_datatype(1.2345678901234567e+19, :float))

    assert_nil NdrAvro::TypeCasting.cast_to_avro_datatype(nil, :float)
  end

  def test_casting_to_double
    assert_in_delta(1.7976931348623157e+308, NdrAvro::TypeCasting.cast_to_avro_datatype('1.7976931348623157e+308', :double))
    assert_in_delta(1.7976931348623157e+308, NdrAvro::TypeCasting.cast_to_avro_datatype(1.7976931348623157e+308, :double))

    assert_nil NdrAvro::TypeCasting.cast_to_avro_datatype(nil, :double)
  end

  # def test_casting_to_bytes
  #   assert_equal 9223372036854775807,
  #                NdrAvro::TypeCasting.cast_to_avro_datatype('9223372036854775807', :bytes)
  #   assert_equal 13, NdrAvro::TypeCasting.cast_to_avro_datatype(13, :bytes)

  #   assert_nil NdrAvro::TypeCasting.cast_to_avro_datatype(nil, :bytes)
  # end

  def test_casting_to_string
    assert_equal '34', NdrAvro::TypeCasting.cast_to_avro_datatype('34', :string)
    assert_equal '35', NdrAvro::TypeCasting.cast_to_avro_datatype(35, :string)

    assert_nil NdrAvro::TypeCasting.cast_to_avro_datatype(nil, :string)
  end

  def test_casting_to_date
    epoch = Date.new(1970, 1, 1)

    assert_equal (Date.new(2021, 4, 28) - epoch).to_i,
                 NdrAvro::TypeCasting.cast_to_avro_datatype('2021-04-28', :date)
    assert_equal 0,
                 NdrAvro::TypeCasting.cast_to_avro_datatype('01/01/1970', :date)
    assert_equal (Date.new(1959, 12, 31) - epoch).to_i,
                 NdrAvro::TypeCasting.cast_to_avro_datatype('1959-12-31', :date)

    assert_nil NdrAvro::TypeCasting.cast_to_avro_datatype(nil, :date)
  end

  def test_casting_to_array
    list_options = { split: ';', items: :int }
    assert_equal [1, 2, 3], NdrAvro::TypeCasting.cast_to_avro_datatype('1;2;3', :array, list_options)
    assert_empty NdrAvro::TypeCasting.cast_to_avro_datatype('', :array, list_options)
    assert_nil NdrAvro::TypeCasting.cast_to_avro_datatype(nil, :array, list_options)
  end

  def test_casting_to_unknown_type
    exception = assert_raises ArgumentError do
      NdrAvro::TypeCasting.cast_to_avro_datatype('12', :unknown_type)
    end
    assert_equal 'Unsupported data type: unknown_type', exception.message

    exception = assert_raises ArgumentError do
      NdrAvro::TypeCasting.cast_to_avro_datatype(nil, :unknown_type)
    end
    assert_equal 'Unsupported data type: unknown_type', exception.message

    unknown_data_type_options = { precision: 3, scale: 1, data_type: :unknown_type }
    exception = assert_raises ArgumentError do
      NdrAvro::TypeCasting.cast_to_avro_datatype('12', unknown_data_type_options)
    end
    assert_equal "Unsupported data type: #{unknown_data_type_options}", exception.message
  end
end
