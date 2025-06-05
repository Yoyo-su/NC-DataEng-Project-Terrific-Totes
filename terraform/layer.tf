# Extract Dependencies (e.g.,pg8000,dotenv etc.)
resource "null_resource" "create_extract_dependencies" {
  provisioner "local-exec" {
    command =  "mkdir -p ${path.module}/../dependencies_extract/python && pip install -r ${path.module}/../requirements_extract.txt -t ${path.module}/../dependencies_extract/python"

  }

  triggers = {
    extract = filemd5("${path.module}/../requirements_extract.txt")
  }
}
resource "null_resource" "zip_extract_layer" {
  provisioner "local-exec" {
    command = "cd ${path.module}/../dependencies_extract && zip -r ../packages/layers/extract_layer.zip ."
  }

  triggers = {
    zipped = timestamp()
  }

  depends_on = [null_resource.create_extract_dependencies]
}

# Extract Layer Archive
data "archive_file" "extract_layer_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/layers/extract_layer.zip"
  source_dir  = "${path.module}/../dependencies_extract/"
  depends_on = [ null_resource.create_extract_dependencies ]
}

resource "aws_lambda_layer_version" "extract_layer" {
  layer_name = "extract-deps-layer"
  s3_bucket  = aws_s3_object.extract_layer_object.bucket
  s3_key     = aws_s3_object.extract_layer_object.key
  
}

# Utils Layer Archive
data "archive_file" "utils_layer_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/layers/utils_layer.zip"
  source_dir  = "${path.module}/../utils/"
  depends_on = [ null_resource.create_extract_dependencies ]
}

resource "aws_lambda_layer_version" "utils_layer" {
  layer_name = "utils-layer"
  s3_bucket  = aws_s3_object.utils_layer_object.bucket
  s3_key     = aws_s3_object.utils_layer_object.key
  
}


