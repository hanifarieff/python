$ScheduledTaskName = ((Get-ScheduledTaskInfo -TaskName "\Sql_LoadBIOS39DMJumlahLayananFarmasi").TaskName)
$Result = ((Get-ScheduledTaskInfo -TaskName "\Sql_LoadBIOS39DMJumlahLayananFarmasi").LastTaskResult)

If ($Result -gt 0) {
    $userName = 'lawkiddd2806@gmail.com'
    $To = 'lawkiddd2806@gmail.com'
    # The 16 digits gmail Acc app password
    $password = 'xoknfjvawuxitjvz'    
    [SecureString]$securepassword = $password | ConvertTo-SecureString -AsPlainText -Force 
    $credential = New-Object System.Management.Automation.PSCredential -ArgumentList $username, $securepassword
    $body = "Your Scheduler : $ScheduledTaskName was Failed with this code $Result"

    Send-MailMessage -SmtpServer smtp.gmail.com -Port 587 -UseSsl -From $userName -To $To -Subject 'Notification Scheduler Error' -Body $body -Credential $credential
}else{
    $userName = 'lawkiddd2806@gmail.com'
    $To = 'lawkiddd2806@gmail.com'
    # The 16 digits gmail Acc app password
    $password = 'xoknfjvawuxitjvz'    
    [SecureString]$securepassword = $password | ConvertTo-SecureString -AsPlainText -Force 
    $credential = New-Object System.Management.Automation.PSCredential -ArgumentList $username, $securepassword
    $body = "Your Scheduler : $ScheduledTaskName success brooo!"

    Send-MailMessage -SmtpServer smtp.gmail.com -Port 587 -UseSsl -From $userName -To $To -Subject 'Notification Scheduler Success' -Body $body -Credential $credential
}