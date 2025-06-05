# s3 bucket for injested data
resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "fscifa-raw-data"

  tags = {
    Name        = "Ingestion Bucket"
    Environment = "Dev"
  }
}

# s3 bucket for processed data
resource "aws_s3_bucket" "ready_bucket" {
  bucket = "fscifa-processed-data"

  tags = {
    Name        = "Ready Bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "code_bucket" {
  bucket = "fscifa-code"

  tags = {
    Name        = "code Bucket"
    Environment = "Dev"
  }
}
resource "aws_s3_object" "lambda_code" {
  for_each = toset([var.extract_lambda, var.transform_lambda])
  bucket   = aws_s3_bucket.code_bucket.bucket
  key      = "${each.key}/function.zip"
  source   = "${path.module}/../packages/${each.key}/function.zip"
  etag     = filemd5("${path.module}/../packages/${each.key}/function.zip")
  depends_on = [ data.archive_file.extract_lambda,data.archive_file.transform_lambda ]
}

resource "aws_s3_object" "extract_layer_object" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "layers/extract_layer.zip"
  source = "${path.module}/../packages/layers/extract_layer.zip"

  depends_on = [null_resource.zip_extract_layer]
}



