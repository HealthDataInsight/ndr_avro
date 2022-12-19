require 'active_model/type'
require 'active_record/type/unsigned_integer'

# See the README for supported Avro column types.

# Unsupported/untested types:
#
# null: no value
# bytes: sequence of 8-bit unsigned bytes

ActiveModel::Type.register(:int) { ActiveModel::Type::Integer.new(limit: 4) }
ActiveModel::Type.register(:long) { ActiveModel::Type::Integer.new(limit: 8) }
ActiveModel::Type.register(:double, ActiveModel::Type::Float)
# ActiveModel::Type.register(:bytes, ActiveModel::Type::Binary)

# ActiveModel::Type.register(:date, ActiveModel::Type::Date)

module NdrAvro
  # This mixin casts values to Avro field types
  class TypeCasting
    SUPPORTED_DATA_TYPES = %i[array boolean date decimal double float int long string].freeze

    def self.cast_to_avro_datatype(value, type, options = {})
      raise ArgumentError, "Unsupported data type: #{type}" if SUPPORTED_DATA_TYPES.exclude?(type)

      return nil if value.nil?

      case type
      when :decimal
        ActiveRecord::Type::Decimal.new(**options).cast(value)
      when :date
        epoch = Date.new(1970, 1, 1)
        date = ActiveModel::Type::Date.new.cast(value)

        ActiveModel::Type::Integer.new(limit: 4).cast((date - epoch).to_i)
      when :array
        cast_to_avro_array(value, options)
      else
        ActiveModel::Type.lookup(type).cast(value)
      end
    end

    def self.cast_to_avro_array(value, options = {})
      value.to_s.split(options.fetch(:split)).map do |v|
        cast_to_avro_datatype(v, options[:items] || :string, options.except(:type))
      end
    end
  end
end
