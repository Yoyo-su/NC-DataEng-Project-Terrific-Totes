# scheduler that will trigger the Lambda every 30 minutes
 resource "aws_cloudwatch_event_rule" "lambda_execution_schedule" {
  name = "every-thirty-minutes"
    description = "Fires every thirty minutes"
    schedule_expression = "rate(30 minutes)"
}

resource "aws_cloudwatch_event_target" "check_quote_handler_every_five_minutes" {
    rule = "${aws_cloudwatch_event_rule.lambda_execution_schedule.name}"
    target_id = "extract_lambda"
    arn = "${aws_lambda_function.extract_lambda.arn}"
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_extract_lambda" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = "${aws_lambda_function.extract_lambda.function_name}"
    principal = "events.amazonaws.com"
    source_arn = "${aws_cloudwatch_event_rule.lambda_execution_schedule.arn}"
}