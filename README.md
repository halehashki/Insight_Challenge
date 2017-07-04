This program is implemented in Python.
Here are the list of librarires used in implementation:
import math
from collections import defaultdict
import operator
import sys
import os


The  src, input and output files should be in the directories formated as (based on coding challange instruction)

├── run.sh
├── src
│   └── process_log.py
├── log_input
│   └── batch_log.json
│   └── stream_log.json
├── log_output
|   └── flagged_purchases.json



The execuatbel code is run.sh,and it sould be in the same directory as src, log_iput and log_output are ( as noted above).To run the program simply use that.For example in mac terminal 
>sh run.sh

the output will be found in log_output folder.



Error checking:
Ignore empty lines in input files.
If the line is not formatted as what is in example, it gives message and exit the program.
If the number of given arguments are not correct, it gives message and exit program.
If batch and stream input files are not exist, it gives message and exit program.
If the customer doesn't have any network of frends, it gives message and exit program.
If there is no previous purchases in customer network including herself,it gives message and exit program.


Tests:

I used the files provided for test_1 in coding challange and for my-own-test I used the sample data provided in the github, so it takes longer compare to test_1. here is what I get running testrun provided in github:

-e [PASS]: test_1
-e [PASS]: your-own-test
[Mon Jul  3 16:44:39 PDT 2017] 2 of 2 tests passed



 


