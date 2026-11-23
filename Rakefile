# frozen_string_literal: true

require 'bundler/gem_tasks'
require 'way_of_working/tasks'
require 'rake/testtask'
require 'ndr_dev_support/tasks'

Rake::TestTask.new(:test) do |t|
  t.libs << 'test'
  t.libs << 'lib'
  t.test_files = FileList['test/**/*_test.rb']
  t.verbose = false
  t.warning = false
end

task default: :test
