import re
my_str = "dev_stg_online"

print(re.findall("stg.*", my_str)[0])