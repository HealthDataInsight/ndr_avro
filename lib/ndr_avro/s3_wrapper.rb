require 'pathname'

module NdrAvro
  # This class enables NdrAvro to run as a lamdba by materialising mappings
  # and copying file from/to input/output S3 buckets.
  class S3Wrapper
    def initialize(safe_dir:)
      @safe_dir = safe_dir
    end

    def materialise_mappings(mappings)
      safe_mapping_path = @safe_dir.join('mapping.yml')
      File.write(safe_mapping_path, mappings)

      safe_mapping_path
    end

    def get_object(bucket, key)
      response = s3.get_object(bucket: bucket, key: key)

      safe_input_path = @safe_dir.join(key)
      File.binwrite(safe_input_path, response.body.read)

      safe_input_path
    end

    def put_object(bucket, path)
      key = path.relative_path_from(@safe_dir).to_s
      response = s3.put_object(body: File.open(path, 'r'), bucket: bucket, key: key)

      {
        bucket: bucket,
        key: key,
        etag: response.etag,
        success: !response.etag.nil?
      }
    end

    private

      def s3
        @s3 ||= Aws::S3::Client.new
      end
  end
end
