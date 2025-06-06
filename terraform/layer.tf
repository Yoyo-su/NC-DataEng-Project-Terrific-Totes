# db Dependencies (e.g.,pg8000,dotenv etc.)
resource "null_resource" "create_dependencies_db" {
  provisioner "local-exec" {
    command =  "mkdir -p ${path.module}/../dependencies_db/python && pip install -r ${path.module}/../requirements_db.txt -t ${path.module}/../dependencies_db/python"

  }

  triggers = {
    db_requirements = filemd5("${path.module}/../requirements_db.txt")
  }
}
resource "null_resource" "zip_db_layer" {
  provisioner "local-exec" {
    command = "cd ${path.module}/../dependencies_db && zip -r ../packages/layers/db_layer.zip"
  }

  triggers = {
    zipped = timestamp()
  }

  depends_on = [null_resource.create_dependencies_db]
}

# db Layer Archive
data "archive_file" "db_layer_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/layers/db_layer.zip"
  source_dir  = "${path.module}/../dependencies_db/"
  depends_on = [ null_resource.create_dependencies_db ]
}

resource "aws_lambda_layer_version" "db_layer" {
  layer_name = "db-deps-layer"
  s3_bucket  = aws_s3_object.db_layer_object.bucket
  s3_key     = aws_s3_object.db_layer_object.key
  
}

# Utils Layer Archive
data "archive_file" "utils_layer_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/layers/utils_layer.zip"
  source_dir  = "${path.module}/../src/"
}

resource "aws_lambda_layer_version" "utils_layer" {
  layer_name = "utils-layer"
  s3_bucket  = aws_s3_object.utils_layer_object.bucket
  s3_key     = aws_s3_object.utils_layer_object.key
  
}


