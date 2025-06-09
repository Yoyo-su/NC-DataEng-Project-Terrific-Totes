resource "aws_scheduler_schedule" "fscifa_etl_schedule" {
  name       = "etl-schedule"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(20 minutes)"

  # start_date = "2025-06-06T12:12:00Z"

  target {
    arn      = aws_sfn_state_machine.sfn_state_machine.arn
    role_arn = aws_iam_role.scheduler_role.arn
  }
}

