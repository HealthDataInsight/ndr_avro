# frozen_string_literal: true

require 'ndr_avro/generator'
require 'ndr_avro/version'

begin
  # Include NdrAvro::S3Wrapper if Aws::S3::Client available
  require 'aws-sdk-s3'

  require 'ndr_avro/s3_wrapper'
rescue LoadError
  # do nothing if gem unavailable
end

# This exposes the root folder for filesystem paths
module NdrAvro
  def self.root
    ::File.expand_path('..', __dir__)
  end
end
