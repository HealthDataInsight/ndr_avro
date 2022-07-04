# frozen_string_literal: true

require_relative 'lib/ndr_avro/version'

Gem::Specification.new do |spec|
  spec.name          = 'ndr_avro'
  spec.version       = NdrAvro::VERSION
  spec.authors       = ['Tim Gentry']
  spec.email         = ['timgentry@users.noreply.github.com']

  spec.summary       = 'NDR Avro File Generator'
  spec.description   = 'Generate avro files using the NDR ETL importer'
  spec.homepage      = 'https://github.com/timgentry/ndr_avro'
  spec.license       = 'MIT'
  spec.required_ruby_version = Gem::Requirement.new('>= 2.7')

  # spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = spec.homepage
  spec.metadata['changelog_uri'] = 'https://github.com/timgentry/ndr_avro/blob/main/CHANGELOG.md'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features)/}) }
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_dependency 'ndr_import'
  # spec.add_dependency 'rake' # rake is an uncaptured red-parquet dependency
  # spec.add_dependency 'red-parquet', '~> 6.0.1'
  spec.add_dependency 'avro', '~> 1.11.0'
  spec.metadata['rubygems_mfa_required'] = 'true'
end
