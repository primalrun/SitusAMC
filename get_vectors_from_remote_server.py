import paramiko

in_file = r'C:\Users\Administrator\Documents\vectors_1st3lines.csv'
remote_server = r'54.161.34.79'
username = r'CO-LTRM--9133\Administrator'
password = r'b*%NxE4-IAws.bOYz8hht;n=ILWsWP!j'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(remote_server, username=username, password=password, timeout=30)
sftp_client = ssh.open_sftp()
remote_file = sftp_client.open(in_file)

try:
    for line in remote_file:
        print(line)
finally:
    remote_file.close()

print('complete')