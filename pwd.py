import os
import getpass

if os.name != 'nt':  # If not Windows
    import pwd
    username = pwd.getpwuid(os.getuid()).pw_name
else:
    # Alternative for Windows
    username = getpass.getuser()

print(f"Username: {username}")