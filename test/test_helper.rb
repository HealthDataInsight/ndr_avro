# frozen_string_literal: true

if ENV['COVERAGE']
  require 'simplecov'
  SimpleCov.start
end

$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'ndr_avro'
require 'ndr_support/safe_path'

require 'minitest/autorun'

SafePath.configure! "#{File.dirname(__FILE__)}/resources/filesystem_paths.yml"
