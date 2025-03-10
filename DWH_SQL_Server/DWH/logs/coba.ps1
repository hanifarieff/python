$userName = 'lawkiddd2806@gmail.com'
$To = 'lawkiddd2806@gmail.com'
# The 16 digits gmail Acc app password
$password = 'ntxiuxluuvhizbns'    
[SecureString]$securepassword = $password | ConvertTo-SecureString -AsPlainText -Force 
$credential = New-Object System.Management.Automation.PSCredential -ArgumentList $username, $securepassword
Send-MailMessage -SmtpServer smtp.gmail.com -Port 587 -UseSsl -From $userName -To $To -Subject 'Test subject' -Body 'Tes Email Berhasil' -Credential $credential




# $EmailFrom = "notifications@somedomain.com"
# $EmailTo = "me@earth.com"
# $Subject = "Notification from XYZ"
# $Body = "this is a notification from XYZ Notifications.."
# $SMTPServer = "smtp.gmail.com"
# $SMTPClient = New-Object Net.Mail.SmtpClient($SmtpServer, 587)
# $SMTPClient.EnableSsl = $true
# $SMTPClient.Credentials = New-Object System.Net.NetworkCredential("username", "password");
# $SMTPClient.Send($EmailFrom, $EmailTo, $Subject, $Body)