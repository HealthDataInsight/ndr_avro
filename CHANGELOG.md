# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v1.0.0-rc.3] - 2022-12-19

### Added

- Switched to safe loading the YAML mapping, allowing only known ruby classes.
- Added ability to yield in NdrAvro::Generator#process, enabling programmatic access to data as it is processed.
- Added ability to specify an Avro file basename which is different to the processed file basename.

### Fixed

- Fixed the GitHub test action

## [v1.0.0-rc.2] - 2022-09-09

### Added

- Added National Disease Registration Service (NDRS) standard mappings
- Added Diagnostic Imaging Data Set (DIDS) test and dummy data
- Added GitHub action to run all the ruby tests

### Fixed

- Actively avoiding broken PdfReader v2.10.0 gem

## [v1.0.0-rc.1] - 2022-07-11

### Added

- Improved Avro date tests

## [0.1.0] - 2022-07-04

- Initial port from ndr_parquet
