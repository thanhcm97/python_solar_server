import time
import os
import json
from collections import deque
import config 


# Function to print debug output

def debug_print(*args):
    log_file = os.path.join(os.path.dirname(__file__), 'debug.log')
    message = ' '.join(str(arg) for arg in args)
    print(message)

    max_lines = config.DEBUG_MAX_LINE
    try:
        # Read the existing lines from the file
        with open(log_file, 'r') as f:
            lines = deque(f.readlines(), maxlen=max_lines)

        # Append the new line to the deque
        lines.append(str(message) + '\n')

        # Write the updated lines back to the file
        with open(log_file, 'w') as f:
            f.writelines(lines)   

    except IOError as e:
        print("An error occurred while accessing the file:", str(e))

    except Exception as e:
        print("An unexpected error occurred:", str(e))
