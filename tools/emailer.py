import smtplib
from email.message import EmailMessage

def email_error(logloc, task, errorstep):
    msg = EmailMessage()
    newline = '\n'
    msg.set_content(f'This is an automatic e-mail generated by {task}.\n'
                    f'\n'
                    f'Errors were encountered during: {errorstep}.\n'
                    f'Please check the log file @ {logloc}\n'
                    f'\n'
                    f'Kind regards,\n'
                    f'Clinical Genomics Gothenburg')

    msg['Subject'] = "ERROR: GENSAM upload"
    msg['From'] = "clinicalgenomics@gu.se"
    msg['To'] = "clinicalgenomics@gu.se"

    #Send the messege
    s = smtplib.SMTP('smtp.gu.se')
    s.send_message(msg)
    s.quit()

def email_micro(subject, body):
    msg = EmailMessage()
    msg.set_content(f'{body}\n'
                    f'\n'
                    f'Kind regards,\n'
                    f'Clinical Genomics Gothenburg')

    msg['Subject'] = f'{subject}'
    msg['From'] = "clinicalgenomics@gu.se"
    msg['To'] = ["johan.ringlander@vgregion.se", "josefin.olausson@vgregion.se"]
    msg['Cc'] = "anders.lind.cgg@gu.se"

    #Send the messege
    s = smtplib.SMTP('smtp.gu.se')
    s.send_message(msg)
    s.quit()
