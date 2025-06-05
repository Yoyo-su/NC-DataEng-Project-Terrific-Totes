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


# Pandas Dependencies
resource "null_resource" "create_pandas_dependencies" {
  provisioner "local-exec" {
    command = <<EOT
      mkdir -p ${path.module}/../dependencies_pandas/python
      pip install -r ${path.module}/../requirements_pandas.txt -t ${path.module}/../dependencies_pandas/python
      find ${path.module}/../dependencies_pandas/python -name "*.pyc" -delete
      find ${path.module}/../dependencies_pandas/python -name "__pycache__" -type d -exec rm -r {} +
      find ${path.module}/../dependencies_pandas/python -name "tests" -type d -exec rm -r {} +
    EOT
  }

  triggers = {
    pandas = filemd5("${path.module}/../requirements_pandas.txt")
  }
}

resource "null_resource" "zip_pandas_layer" {
  provisioner "local-exec" {
    
    command = "cd ${path.module}/../dependencies_pandas && zip -r ../packages/layers/pandas_layer.zip ."
  }

  triggers = {
    time = timestamp()
  }

  depends_on = [null_resource.create_pandas_dependencies]
}

# Forex and Fastparquet Dependencies
resource "null_resource" "create_forex_parquet_dependencies" {
  provisioner "local-exec" {
    command = <<EOT
      pip install -r ${path.module}/../requirements_forex_fastparquet.txt -t ${path.module}/../dependencies_pandas/python
      find ${path.module}/../dependencies_pandas/python -name "*.pyc" -delete
      find ${path.module}/../dependencies_pandas/python -name "__pycache__" -type d -exec rm -r {} +
      find ${path.module}/../dependencies_pandas/python -name "tests" -type d -exec rm -r {} +
    EOT
  }

  triggers = {
    forex_parquet = filemd5("${path.module}/../requirements_forex_fastparquet.txt")
  }
}

resource "null_resource" "zip_forex_parquet_layer" {
  provisioner "local-exec" {
    command = "cd ${path.module}/../dependencies_pandas && zip -r ../packages/layers/forex_parquet_layer.zip ."
  }

  triggers = {
    time = timestamp()
  }

  depends_on = [null_resource.create_forex_parquet_dependencies]
}

# Extract Layer Archive
data "archive_file" "extract_layer_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/layers/extract_layer.zip"
  source_dir  = "${path.module}/../dependencies_extract/"
  depends_on = [ null_resource.create_extract_dependencies ]
}

# Pandas Layer Archive
data "archive_file" "pandas_layer_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/layers/pandas_layer.zip"
  source_dir  = "${path.module}/../dependencies_pandas/"
  depends_on = [ null_resource.create_pandas_dependencies ]
}

# Forex & Fastparquet Layer Archive
data "archive_file" "forex_parquet_layer_zip" {
  type        = "zip"
  output_path = "${path.module}/../packages/layers/forex_parquet_layer.zip"
  source_dir  = "${path.module}/../dependencies_pandas/"
  depends_on = [ null_resource.create_forex_parquet_dependencies ]
}

resource "aws_lambda_layer_version" "extract_layer" {
  layer_name = "extract-deps-layer"
  s3_bucket  = aws_s3_object.extract_layer_object.bucket
  s3_key     = aws_s3_object.extract_layer_object.key
  
}

resource "aws_lambda_layer_version" "pandas_layer" {
  layer_name = "pandas-deps-layer"
  s3_bucket  = aws_s3_object.pandas_layer_object.bucket
  s3_key     = aws_s3_object.pandas_layer_object.key
  
}

resource "aws_lambda_layer_version" "forex_parquet_layer" {
  layer_name = "forex-parquet-deps-layer"
  s3_bucket  = aws_s3_object.forex_parquet_layer_object.bucket
  s3_key     = aws_s3_object.forex_parquet_layer_object.key
  
}
