# $TaskFactAppointmentEHR = (Get-ScheduledTask -TaskName "\Sql_LoadFactAppointmentQueueEHR" | Get-ScheduledTaskInfo).LastTaskResult
# $ScheduledTaskName = ((Get-ScheduledTaskInfo -TaskName "\Sql_LoadFactAppointmentQueueEHR").TaskName)
$Result = ((Get-ScheduledTaskInfo -TaskName "\Sql_LoadFactAppointmentQueueEHR").LastTaskResult)

# if ($TaskFactAppointmentEHR -ne 0 -or $TaskFactAppointmentHIS -ne 0) {
#     # Both tasks have failed
#     $failedTasks = ""
#     if ($TaskFactAppointmentEHR -ne 0) { $failedTasks += "Task Fact Apppointment EHR " }
#     if ($TaskFactAppointmentHIS -ne 0) { $failedTasks += "Task Fact Apppointment HIS " }

#     # Send email notification
#     $userName = 'lawkiddd2806@gmail.com'
#     $To = 'lawkiddd2806@gmail.com'
#     # The 16 digits gmail Acc app password
#     $password = 'xoknfjvawuxitjvz'    
#     [SecureString]$securepassword = $password | ConvertTo-SecureString -AsPlainText -Force 
#     $credential = New-Object System.Management.Automation.PSCredential -ArgumentList $username, $securepassword
#     $subject = "One or more tasks have failed"
#     $body = "The following task(s) have failed: $failedTasks"

#     Send-MailMessage -SmtpServer smtp.gmail.com -Port 587 -UseSsl -From $userName -To $To -Subject $subject -Body $body -Credential $credential
    
# } else {
#     # Both tasks have succeeded
#     $userName = 'lawkiddd2806@gmail.com'
#     $To = 'lawkiddd2806@gmail.com'
#     # The 16 digits gmail Acc app password
#     $password = 'xoknfjvawuxitjvz'    
#     [SecureString]$securepassword = $password | ConvertTo-SecureString -AsPlainText -Force 
#     $credential = New-Object System.Management.Automation.PSCredential -ArgumentList $username, $securepassword
#     $subject = "Both tasks have succeeded"
#     $body = "Both Task A and Task B have completed successfully"
#     Send-MailMessage -SmtpServer smtp.gmail.com -Port 587 -UseSsl -From $userName -To $To -Subject $subject -Body $body -Credential $credential

# }

If ($Result -gt 0) {
    $userName = 'lawkiddd2806@gmail.com'
    $To = 'lawkiddd2806@gmail.com'
    # The 16 digits gmail Acc app password
    $password = 'xoknfjvawuxitjvz'    
    [SecureString]$securepassword = $password | ConvertTo-SecureString -AsPlainText -Force 
    $credential = New-Object System.Management.Automation.PSCredential -ArgumentList $username, $securepassword
    $body = "Your Scheduler FactAppointmentQueue EHR was Failed "
    Send-MailMessage -SmtpServer smtp.gmail.com -Port 587 -UseSsl -From $userName -To $To -Subject 'Notification Scheduler Error' -Body $body -Credential $credential
}
else 
{
    $userName = 'lawkiddd2806@gmail.com'
    $To = 'lawkiddd2806@gmail.com'
    # The 16 digits gmail Acc app password
    $password = 'xoknfjvawuxitjvz'    
    [SecureString]$securepassword = $password | ConvertTo-SecureString -AsPlainText -Force 
    $credential = New-Object System.Management.Automation.PSCredential -ArgumentList $username, $securepassword
    $body = "Your Scheduler FactAppointmentQueue EHR success brooo!"  
    Send-MailMessage -SmtpServer smtp.gmail.com -Port 587 -UseSsl -From $userName -To $To -Subject 'Notification Scheduler Success' -Body $body -Credential $credential
}