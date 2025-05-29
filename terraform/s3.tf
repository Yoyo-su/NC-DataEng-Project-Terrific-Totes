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

# s3 bucket for storing lambda functions
resource "aws_s3_bucket" "lambda_bucket" {
  bucket = "fscifa-lamdba"

  tags = {
    Name        = "Lambda Bucket"
    Environment = "Dev"
  }
}
