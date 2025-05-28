resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "fscifa-raw-data"

  tags = {
    Name        = "Ingestion Bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "ready_bucket" {
  bucket = "fscifa-processed-data"

  tags = {
    Name        = "Ready Bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "lambda_bucket" {
  bucket = "fscifa-lamdba"

  tags = {
    Name        = "Lambda Bucket"
    Environment = "Dev"
  }
}
