require 'avro'

module NdrAvro
  class Generator
    # Generator mixin to create and save Arrow tables as avro files
    module AvroFileHelper
      def self.included(base)
        base.class_eval do
          attr_reader :output_files
        end
      end

      private

        def avro_filename(klass, mode)
          @output_path.join("#{@basename}.#{klass.underscore}.#{mode}.avro")
        end

        def avro_schema_filename(klass, mode)
          @output_path.join("#{@basename}.#{klass.underscore}.#{mode}.avsc")
        end

        def avro_schema_type(type, options)
          case type
          when :decimal
            {
              type: 'bytes', logicalType: 'decimal',
              precision: options[:precision], scale: options[:scale]
            }
          when :array
            { type: 'array', items: options[:items] || 'string' }
          when :date
            { type: 'int', logicalType: 'date' }
          else
            type
          end
        end

        def mapped_json_schema(klass)
          fields = @avro_column_types[klass].to_a.map do |fieldname, definition|
            type = definition[:type]
            options = definition[:options]

            schema_type = avro_schema_type(type, options)

            {
              name: fieldname,
              type: [schema_type, 'null']
            }
          end

          {
            type: 'record',
            name: klass,
            fields: fields
          }.to_json
        end

        def raw_json_schema(klass)
          fields = @rawtext_column_names[klass].to_a.map do |fieldname|
            {
              name: fieldname,
              type: %w[string null]
            }
          end

          {
            type: 'record',
            name: klass,
            fields: fields
          }.to_json
        end

        def save_mapped_avro_files(klass_mapped_hashes)
          klass_mapped_hashes.each do |klass, mapped_hashes|
            # Save the mapped avro file
            schema = mapped_json_schema(klass)

            rows = mapped_hashes.map do |mapped_hash|
              @avro_column_types[klass].each do |fieldname, definition|
                # Unfortunately, Avro can't do it appropriate casting implicitly.
                value = mapped_hash[fieldname]
                mapped_hash[fieldname] =
                  TypeCasting.cast_to_avro_datatype(value, definition[:type], definition[:options])
              end
              mapped_hash
            end

            save_and_log_avro_file(klass, schema, rows, :mapped)
          end
        end

        def save_raw_avro_files(klass_rawtext_hashes)
          klass_rawtext_hashes.each do |klass, rawtext_hashes|
            # Save the rawtext avro file
            schema = raw_json_schema(klass)

            rows = rawtext_hashes.map do |rawtext_hash|
              rawtext_hash
              # @rawtext_column_names[klass].to_a.map { |fieldname| rawtext_hash[fieldname] }
            end

            save_and_log_avro_file(klass, schema, rows, :raw)
          end
        end

        def save_and_log_avro_file(klass, schema, rows, mode)
          output_filename = avro_filename(klass, mode)
          file = File.open(output_filename, 'wb')

          # schema = Avro::Schema.parse(File.open("item.avsc", "rb").read)
          # Save schema
          schema_filename = avro_schema_filename(klass, mode)
          File.write(schema_filename, schema)

          schema = Avro::Schema.parse(schema)

          # Creates DatumWriter instance with required schema.
          writer = Avro::IO::DatumWriter.new(schema)

          # Below dw is equivalent to DataFileWriter instance in Java API
          dw = Avro::DataFile::Writer.new(file, writer, schema)

          # write each record into output avro data file
          rows.each do |row|
            dw << row
          end

          # close the avro data file
          dw.close
          file.close

          @output_files ||= []
          @output_files << {
            path: output_filename,
            schema: schema_filename,
            total_rows: rows.length
          }
        end
    end
  end
end
