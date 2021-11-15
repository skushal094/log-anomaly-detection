import sys
import pandas as pd
import numpy as np
import logging
from Drain import LogParser

# count rows
with open('./../data/HDFS.log', "r") as file:
    length = 0
    for line in file:
        length += 1

print('There are a total of {} lines'.format(length))

# Take a quick look at the data:

data = []
with open('./../data/HDFS.log', "r") as file:
    n = 0
    for line in file:
        data.append(line)
        if n < 200:
            n += 1
        else:
            break

df = pd.DataFrame(data)
print("First 5 rows of log:")
print(df.iloc[0:5].values)

# Training and Testing .log Files

train_idx = int(length * 0.8)
print(f"training data index limit: {train_idx}")

# read the training lines only
train_data = []
with open('./../data/HDFS.log', "r") as file:
    n = 0
    for line in file:
        if n < train_idx:
            train_data.append(line)
            n += 1
        else:
            break

# write to the training file `HDFS_train.log`
with open('./../data/HDFS_train.log', 'w') as file:
    for i in train_data:
        file.write(i)


input_dir = "./../data"  # The input directory of log file
output_dir = "./../data"  # The output directory of parsing results
log_file_all = "HDFS.log"  # The input log file name
log_file_train = "HDFS_train.log"  # The input log file name containing only the training data
log_format = "<Date> <Time> <Pid> <Level> <Component>: <Content>"  # HDFS log format
# Regular expression list for optional preprocessing (default: [])
regex = [
    r"blk_(|-)[0-9]+",  # block id
    r"(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)",  # IP
    r"(?<=[^A-Za-z0-9])(\-?\+?\d+)(?=[^A-Za-z0-9])|[0-9]+$",  # Numbers
]
st = 0.5  # Similarity threshold
depth = 4  # Depth of all leaf nodes

# run on training dataset
parser = LogParser(
    log_format, indir=input_dir, outdir=output_dir, depth=depth, st=st, rex=regex
)
parser.parse(log_file_train)

# run on complete dataset
parser = LogParser(
    log_format, indir=input_dir, outdir=output_dir, depth=depth, st=st, rex=regex
)
parser.parse(log_file_all)

# create parsed test data
all_parsed = pd.read_csv('./../data/HDFS.log_structured.csv')
test_parsed = all_parsed.iloc[train_idx:]
test_parsed.to_csv('./../data/HDFS_test.log_structured.csv', index=False)
