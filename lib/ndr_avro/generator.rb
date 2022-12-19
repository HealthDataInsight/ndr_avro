# frozen_string_literal: true

require 'ndr_import'
require 'ndr_import/universal_importer_helper'
require 'ndr_avro/type_casting'
require 'ndr_avro/generator/avro_file_helper'
require 'pathname'

module NdrAvro
  # Reads file using NdrImport ETL logic and creates avro file(s)
  class Generator
    include NdrImport::UniversalImporterHelper
    include NdrAvro::Generator::AvroFileHelper

    def initialize(filename, table_mappings, options = {})
      @filename = filename
      load_mappings(table_mappings)
      @output_path = Pathname.new(options[:output_path] || '')
      @basename = options[:basename] || File.basename(filename, File.extname(filename))
      @rawtext_column_names = {}
      @avro_column_types = {}

      ensure_all_mappings_are_tables
    end

    def process
      mapped_hashes = {}
      rawtext_hashes = {}

      extract(@filename).each do |table, rows|
        capture_all_rawtext_names(table)
        capture_all_avro_column_types(table)

        table.transform(rows).each do |instance, fields, index|
          klass = instance.split('#').first

          yield(klass, instance, fields, index) if block_given?

          mapped_hashes[klass] ||= []
          mapped_hashes[klass] << fields.except(:rawtext)

          rawtext_hashes[klass] ||= []
          rawtext_hashes[klass] << fields[:rawtext]
        end
      end

      save_avro_files(mapped_hashes, rawtext_hashes)
    end

    private

      def load_mappings(table_mappings)
        return unless File.exist?(table_mappings)

        File.open(table_mappings, 'r:bom|utf-8') do |f|
          permitted_classes = [
            NdrImport::NonTabular::Table,
            NdrImport::Table,
            Range,
            Regexp,
            RegexpRange,
            Symbol
          ]
          @table_mappings = YAML.safe_load(f, filename: table_mappings,
                                              permitted_classes: permitted_classes)
        end
      end

      def ensure_all_mappings_are_tables
        return if @table_mappings.all? { |table| table.is_a?(NdrImport::Table) }

        raise 'Mappings must be inherit from NdrImport::Table'
      end

      def unzip_path
        @unzip_path ||= SafePath.new('unzip_path')
      end

      def get_notifier(_value); end

      def each_masked_mapping(table)
        masked_mappings = table.send(:masked_mappings)
        masked_mappings.each do |instance, columns|
          klass = instance.split('#').first

          yield klass, columns
        end
      end

      def capture_all_avro_column_types(table)
        each_masked_mapping(table) do |klass, columns|
          @avro_column_types[klass] ||= {}

          columns.each do |column|
            next if column['mappings'].nil? || column['mappings'] == []

            column['mappings'].each do |mapping|
              field = mapping['field']

              column_definition = {
                type: mapping['avro_data_type'] || :string,
                options: mapping['avro_data_type_options']&.symbolize_keys!
              }
              add_or_compare_column_definition(klass, field, column_definition)
            end
          end
        end
      end

      def add_or_compare_column_definition(klass, field, definition)
        if @avro_column_types[klass].include?(field)
          # Check definitions are the same
          if @avro_column_types[klass][field] != definition
            raise "Different Avro column type definitions for #{field}"
          end
        else
          @avro_column_types[klass][field] = definition
        end
      end

      def capture_all_rawtext_names(table)
        each_masked_mapping(table) do |klass, columns|
          @rawtext_column_names[klass] ||= Set.new

          columns.each do |column|
            rawtext_column_name = column[NdrImport::Mapper::Strings::RAWTEXT_NAME] ||
                                  column[NdrImport::Mapper::Strings::COLUMN]

            next if rawtext_column_name.nil?

            @rawtext_column_names[klass] << rawtext_column_name.downcase
          end
        end
      end
  end
end
