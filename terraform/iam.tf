# Role and policy for Extract Lambda to Read/Write S3 Injestion bucket

resource "aws_iam_role" "lambda_extract_role" {
  name = "lambda_extract_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_policy" "s3_write_policy" {
  name = "s3-policy-extract-lambda-write"
  policy      = data.aws_iam_policy_document.allow_extraction_lambda_access_s3_injestion.json
}

data "aws_iam_policy_document" "allow_extraction_lambda_access_s3_injestion" {
  statement {

    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
    ]

    resources = [
      aws_s3_bucket.ingestion_bucket.arn,
      "${aws_s3_bucket.ingestion_bucket.arn}/*",
    ]
  }
}

resource "aws_iam_policy_attachment" "lambda_s3_policy" {
  name       = "lambda_s3_policy"
  roles      = [aws_iam_role.lambda_extract_role.name]
  policy_arn = aws_iam_policy.s3_write_policy.arn
}